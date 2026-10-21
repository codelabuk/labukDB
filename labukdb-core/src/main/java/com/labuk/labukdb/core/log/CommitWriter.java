package com.labuk.labukdb.core.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.RemoveFile;
import com.labuk.labukdb.core.log.action.SingleAction;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Appends one new data file to an existing Delta table: encodes a batch,
 * writes it under the table's Hive-style partition layout, and commits it
 * as a single {@code add} action in the next version's JSON commit file.
 *
 * <b>Scope, stated plainly:</b> local disk only
 * ({@link HadoopLogStorage#createCommitFile} throws rather than pretending
 * remote writes are safe — see its javadoc, and
 * {@code docs/labukdb-v2-architecture.md}'s "Concurrent writes" section
 * for why remote multi-writer OCC needs its own, separate verification
 * pass); appends to an existing table only, no table creation.
 *
 * <b>Multi-writer concurrency (local disk):</b> both write methods retry
 * under optimistic concurrency control when {@link LogStorage#createCommitFile}'s
 * atomic create loses a race for a version number — see
 * {@link #commitWithRetry} for the shared retry shape, and each method's
 * own javadoc for why {@code appendBatch} always retries unconditionally
 * (a blind append never conflicts with another) while {@code replaceFiles}
 * re-checks its removed files on every attempt (it isn't safe to retry
 * blindly once something it depends on has changed).
 *
 * {@code AddFile.stats} is left {@code null} on every file this writes —
 * the existing fail-open pruning design already treats missing stats as
 * "keep the file," so this is a safe simplification, not a correctness
 * gap. Real stats collection is a later refinement.
 */
public final class CommitWriter {

    private final Path tableRoot;
    private final LocalNioLogStorage storage;
    private final SnapshotManager snapshotManager;
    private final ArrowToParquetWriter parquetWriter = new ArrowToParquetWriter();
    private final ObjectMapper mapper = new ObjectMapper();

    public CommitWriter(Path tableRoot) {
        this.tableRoot = tableRoot;
        this.storage = new LocalNioLogStorage(tableRoot.resolve("_delta_log"));
        this.snapshotManager = new SnapshotManager(tableRoot);
    }

    /**
     * Encodes {@code batch} to a new Parquet file under the table's
     * partition layout (as given by {@code partitionValues} — not
     * inferred from the batch's data) and commits it as the next version.
     *
     * @param batch           the rows to write; only the table's physical
     *                        (non-partition) columns — partition columns
     *                        are supplied separately, not expected in the
     *                        batch, matching how they're reconstructed on
     *                        read (see {@code PartitionValueAppender}).
     * @param partitionValues this file's partition values, in whatever
     *                        order; mapped to the table's actual
     *                        partition-column order internally
     * @return the new table version
     */
    public long appendBatch(VectorSchemaRoot batch, Map<String, String> partitionValues) throws IOException {
        Snapshot initial = snapshotManager.latest();
        AddFile addFile = encodeToNewFile(initial, batch, partitionValues, true);

        // A blind append only ever adds a new file — it never reads or
        // depends on any existing file's content, so it's always safe to
        // rebase onto a newer version and retry: two concurrent blind
        // appends never conflict at the data level, only at who gets
        // which version number. No per-attempt check needed here, unlike
        // replaceFiles below.
        return commitWithRetry(current -> List.of(SingleAction.ofAdd(addFile)));
    }

    /**
     * Replaces {@code filesToRemove} with one new file encoded from
     * {@code mergedBatch}, as a single atomic commit — what {@code Compactor}
     * uses. Every action in this commit, both the removes and the add, has
     * {@code dataChange=false}: compaction doesn't change the table's
     * logical data, only which physical file holds it, which is exactly
     * what lets incremental/change-data-feed readers safely skip a
     * compaction commit entirely rather than reprocessing unchanged data.
     *
     * Removing the log entries here does not delete the old files' bytes
     * from disk — that's {@code Vacuum}'s job, deliberately separate, so
     * an in-flight reader that resolved its snapshot before this commit
     * can still legally read a file this commit just removed.
     *
     * <b>A known, minor tradeoff from retrying only the commit, not the
     * encode:</b> {@code mergedBatch} is encoded to a physical Parquet
     * file once, up front, before any retry/conflict-check attempt —
     * deliberately, so a retried commit doesn't re-encode and rewrite the
     * same bytes on every attempt. If this method ultimately throws
     * {@link ConcurrentModificationException} (a genuine conflict, not a
     * retryable one), that one already-written file is never referenced
     * by any commit — an orphan. This project's {@code Vacuum} doesn't
     * detect orphans today (it only ever acts on files a {@code remove}
     * action named) — the same gap real Iceberg's separate
     * {@code DeleteOrphanFiles} action covers and this project doesn't
     * yet. A real but narrow cost: one wasted file per failed compaction
     * attempt, not a correctness issue for the table itself.
     *
     * @return the new table version
     */
    public long replaceFiles(Collection<AddFile> filesToRemove, VectorSchemaRoot mergedBatch,
                              Map<String, String> partitionValues) throws IOException {
        Snapshot initial = snapshotManager.latest();
        AddFile addFile = encodeToNewFile(initial, mergedBatch, partitionValues, false);

        // Unlike appendBatch, this DOES depend on filesToRemove still being
        // present — re-checked against the freshly-resolved snapshot on
        // every attempt (not just the first), since a conflict can cascade:
        // resolve, lose the race, re-resolve, lose again. If a file we're
        // trying to remove is already gone, someone else beat us to
        // removing/compacting it — that's a genuine conflict, not
        // something safe to blindly retry against stale data.
        return commitWithRetry(current -> {
            Set<String> activePaths = current.getActiveFiles().stream()
                    .map(AddFile::getPath).collect(Collectors.toSet());
            for (AddFile removed : filesToRemove) {
                if (!activePaths.contains(removed.getPath())) {
                    throw new ConcurrentModificationException(
                            "Compaction conflict: '" + removed.getPath() + "' was already removed "
                                    + "by a concurrent commit — refusing to retry against stale data");
                }
            }
            List<SingleAction> actions = new ArrayList<>();
            for (AddFile removed : filesToRemove) {
                actions.add(SingleAction.ofRemove(new RemoveFile(removed.getPath(), System.currentTimeMillis(), false)));
            }
            actions.add(SingleAction.ofAdd(addFile));
            return actions;
        });
    }

    private static final int MAX_RETRIES = 10;

    @FunctionalInterface
    private interface ActionsBuilder {
        List<SingleAction> build(Snapshot current);
    }

    /**
     * Shared retry shape for both write methods: re-resolve the latest
     * snapshot, ask {@code actionsBuilder} for this attempt's actions
     * (which may throw {@link ConcurrentModificationException} to signal
     * a genuine, non-retryable conflict — see {@link #replaceFiles}),
     * attempt the atomic commit, and on {@link FileAlreadyExistsException}
     * (someone else won this version number) loop back and try again,
     * bounded by {@link #MAX_RETRIES}.
     */
    private long commitWithRetry(ActionsBuilder actionsBuilder) throws IOException {
        FileAlreadyExistsException lastConflict = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                backoff(attempt);
            }
            Snapshot current = snapshotManager.latest();
            long nextVersion = current.getVersion() + 1;
            List<SingleAction> actions = actionsBuilder.build(current);

            StringBuilder commit = new StringBuilder();
            for (SingleAction action : actions) {
                commit.append(mapper.writeValueAsString(action)).append('\n');
            }
            try {
                storage.createCommitFile(CommitFileNames.jsonFileName(nextVersion), commit.toString());
                return nextVersion;
            } catch (FileAlreadyExistsException conflict) {
                lastConflict = conflict;
                // another writer won nextVersion first -- loop back, re-resolve, retry
            }
        }
        throw new IOException("Too much write contention on " + tableRoot
                + " -- failed to commit after " + MAX_RETRIES + " attempts", lastConflict);
    }

    /** Small randomized backoff so retrying writers don't lock-step collide on every attempt. */
    private void backoff(int attempt) {
        try {
            Thread.sleep(Math.min(50, 2L << attempt) + (long) (Math.random() * 10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Shared by {@link #appendBatch} and {@link #replaceFiles} — encodes a batch under the table's partition layout, doesn't commit anything itself. */
    private AddFile encodeToNewFile(Snapshot current, VectorSchemaRoot batch, Map<String, String> partitionValues,
                                     boolean dataChange) throws IOException {
        String relativePath = buildRelativePath(current, partitionValues);
        File dataFile = tableRoot.resolve(relativePath).toFile();
        Files.createDirectories(dataFile.getParentFile().toPath());
        parquetWriter.write(dataFile, batch);

        return new AddFile(
                relativePath.replace('\\', '/'),
                partitionValues,
                dataFile.length(),
                System.currentTimeMillis(),
                dataChange,
                null);
    }

    /**
     * {@code col1=val1/col2=val2/part-<uuid>.snappy.parquet}, ordered by
     * the table's actual partition-column order (not whatever order the
     * caller's map happens to iterate in) — mirrors what's already on
     * disk for every existing table, though nothing in this codebase
     * actually parses this convention back out; any unique path under the
     * table root would be equally correct. {@code CommitFileNames} only
     * governs {@code _delta_log/} filenames, not data file paths.
     */
    private String buildRelativePath(Snapshot current, Map<String, String> partitionValues) {
        StringBuilder dir = new StringBuilder();
        for (String column : current.getMetadata().getPartitionColumns()) {
            String value = partitionValues.get(column);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Missing value for partition column '" + column + "'; table partition columns are "
                                + current.getMetadata().getPartitionColumns());
            }
            dir.append(column).append('=').append(value).append('/');
        }
        String fileName = "part-" + UUID.randomUUID() + ".snappy.parquet";
        return dir + fileName;
    }
}
