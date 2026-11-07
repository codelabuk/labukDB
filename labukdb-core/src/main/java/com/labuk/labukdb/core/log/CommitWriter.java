package com.labuk.labukdb.core.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.RemoveFile;
import com.labuk.labukdb.core.log.action.SingleAction;
import com.labuk.labukdb.core.storage.RemoteWriteSafetyWarning;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Appends one new data file to an existing Delta table: encodes a batch,
 * writes it under the table's Hive-style partition layout, and commits it
 * as a single {@code add} action in the next version's JSON commit file.
 *
 * <b>Scope, stated plainly:</b> appends to an existing table only, no
 * table creation (see {@link TableCreator} for that). Remote
 * (S3/MinIO/ADLS) append is supported via {@link #forLocation} — live
 * MinIO-verified; multi-writer OCC itself (the retry logic below) is
 * only exercised there the same way the local-disk primitive is, not
 * separately stress-tested against concurrent remote writers — see
 * {@code docs/ARCHITECTURE.md}'s "Concurrent writes" section.
 *
 * <b>Multi-writer concurrency:</b> both write methods retry
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

    private final TableLocation tableLocation;
    private final LogStorage storage;
    private final SnapshotManager snapshotManager;
    private final LockProvider lockProvider;
    private final ArrowToParquetWriter parquetWriter = new ArrowToParquetWriter();
    private final ObjectMapper mapper = new ObjectMapper();

    /** Local disk — unchanged behavior from before {@link TableLocation} existed. */
    public CommitWriter(Path tableRoot) throws IOException {
        this(TableLocation.forLocal(tableRoot), null);
    }

    private CommitWriter(TableLocation location, LockProvider lockProvider) throws IOException {
        this.tableLocation = location;
        this.storage = LogStorage.forLocation(location);
        this.snapshotManager = SnapshotManager.forLocation(location);
        this.lockProvider = lockProvider;
        RemoteWriteSafetyWarning.warnIfUnsafe(location, lockProvider != null, "Delta");
    }

    /**
     * Dispatches on the location — local or remote (S3/MinIO/ADLS) — the
     * same {@link TableLocation}-based entry point {@link SnapshotManager}
     * and {@code decode/ParquetToArrowReader} already use for reads. This
     * is what {@code WriteBuffer}/{@code LabukDbFlightProducer} use so a
     * {@code DoPut} against a remote table works the same way a local one
     * always has.
     *
     * <b>No external lock</b> — safe for a single remote writer only (see
     * {@link #forLocation(TableLocation, LockProvider)} for genuine
     * multi-writer safety on object storage whose conditional-create isn't
     * trustworthy under concurrency, confirmed broken on at least one real
     * deployment; see {@link LockProvider}'s javadoc).
     */
    public static CommitWriter forLocation(TableLocation location) throws IOException {
        return new CommitWriter(location, null);
    }

    /**
     * Same as {@link #forLocation(TableLocation)}, but every commit is
     * mediated through {@code lockProvider} — the fix for a real, confirmed
     * gap in plain object-store conditional-create under genuine
     * concurrency (see {@link LockProvider}'s javadoc). Opt-in: every
     * writer of a given table must use the same {@link LockProvider} (same
     * backing table) for this to actually provide mutual exclusion — one
     * writer bypassing it defeats the scheme for that table.
     */
    public static CommitWriter forLocation(TableLocation location, LockProvider lockProvider) throws IOException {
        return new CommitWriter(location, lockProvider);
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
     * <b>{@code dataChange}</b> — {@code false} for compaction ({@code
     * Compactor}'s own call site), since bin-packing doesn't change the
     * table's logical data, only which physical file holds it; {@code
     * true} for a key-level merge/upsert ({@code TableMerger}), since row
     * values genuinely change. Incremental/change-data-feed readers rely
     * on this distinction to safely skip a compaction commit without
     * reprocessing unchanged data — getting it wrong for a real data
     * change would silently hide that change from such a reader.
     *
     * <b>{@code mergedBatch} with zero rows</b> commits removes-only, no
     * add action — needed when every row in {@code filesToRemove} was
     * deleted and nothing survived. {@code Compactor} never hits this
     * (it only runs when files already exist to merge); additive for
     * {@code TableMerger}'s "deleted everything in this partition" case.
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
                              Map<String, String> partitionValues, boolean dataChange) throws IOException {
        AddFile addFile;
        if (mergedBatch.getRowCount() == 0) {
            addFile = null;
        } else {
            Snapshot initial = snapshotManager.latest();
            addFile = encodeToNewFile(initial, mergedBatch, partitionValues, dataChange);
        }

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
            if (addFile != null) {
                actions.add(SingleAction.ofAdd(addFile));
            }
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
     * attempt the atomic commit, and on a conflict (someone else won this
     * version number) loop back and try again, bounded by {@link #MAX_RETRIES}.
     *
     * <b>Catches two unrelated exception classes, deliberately:</b>
     * {@link FileAlreadyExistsException} (local, NIO) and
     * {@code org.apache.hadoop.fs.FileAlreadyExistsException} (remote,
     * Hadoop) are not related by inheritance despite the same simple name
     * — see {@link HadoopLogStorage}'s javadoc. Before {@link #storage}
     * became {@link TableLocation}-dispatched, only the NIO one could
     * ever actually occur here; now either can, depending on {@code location}.
     */
    private long commitWithRetry(ActionsBuilder actionsBuilder) throws IOException {
        IOException lastConflict = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                backoff(attempt);
            }
            if (lockProvider != null) {
                // Must run BEFORE resolving `current` below: a plain file
                // listing can't see that a *previous* writer already
                // claimed (and maybe finished, maybe crashed on) the very
                // next version slot via the lock -- only the lock itself
                // knows that. Recovering any stuck entries first makes the
                // listing below accurate, exactly mirroring why real
                // Delta's BaseExternalLogStore treats the external store,
                // not a directory listing, as the source of truth for
                // "what's the real latest version."
                catchUpViaLock(lockProvider);
            }
            Snapshot current = snapshotManager.latest();
            long nextVersion = current.getVersion() + 1;
            List<SingleAction> actions = actionsBuilder.build(current);

            StringBuilder commit = new StringBuilder();
            for (SingleAction action : actions) {
                commit.append(mapper.writeValueAsString(action)).append('\n');
            }
            try {
                String fileName = CommitFileNames.jsonFileName(nextVersion);
                if (lockProvider != null) {
                    commitViaLock(lockProvider, fileName, commit.toString());
                } else {
                    storage.createCommitFile(fileName, commit.toString());
                }
                return nextVersion;
            } catch (FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException conflict) {
                lastConflict = conflict;
                // another writer won nextVersion first -- loop back, re-resolve, retry
            }
        }
        throw new IOException("Too much write contention on " + tableLocation
                + " -- failed to commit after " + MAX_RETRIES + " attempts", lastConflict);
    }

    private static final long DEFAULT_LOCK_EXPIRY_SECONDS = 86_400; // 24h, matches Delta's own default entry-expiration delay
    private static final int MAX_RECOVERY_ATTEMPTS = 3;

    /**
     * Scans forward from {@code snapshotManager.latest()}'s version + 1,
     * recovering any stuck (incomplete) lock entries, until it finds a
     * version with no lock entry at all — a genuinely free slot. This is
     * what makes the lock provider (not a plain file listing) the real
     * source of truth for "what's the next version," matching why real
     * Delta's {@code BaseExternalLogStore} overrides {@code listFrom} to
     * consult the external store rather than trusting a directory listing
     * alone: a previous writer may have claimed the very next slot (and
     * crashed, or simply not finished yet) in a way a plain listing can't
     * see until that version's file actually exists.
     *
     * <b>A version check here checking only N-1 (not N itself) would be
     * wrong</b> — found and fixed during design, not assumed correct: if
     * the stuck entry is for the version this writer is about to attempt,
     * checking only N-1 never notices it, the plain listing never advances
     * past the stuck slot, and every retry recomputes the exact same
     * {@code nextVersion}, livelocking against {@link #MAX_RETRIES} instead
     * of self-healing.
     */
    private void catchUpViaLock(LockProvider lock) throws IOException {
        String tablePath = tableLocation.raw();
        long candidate = snapshotManager.latest().getVersion() + 1;
        while (true) {
            Optional<ExternalCommitEntry> existing = lock.getEntry(tablePath, CommitFileNames.jsonFileName(candidate));
            if (existing.isEmpty()) {
                return; // genuinely free slot
            }
            if (!existing.get().complete()) {
                recoverEntry(existing.get());
            }
            candidate++;
        }
    }

    /**
     * The real fix for the confirmed MinIO conditional-create gap — mirrors
     * real Delta Lake's {@code io.delta.storage.BaseExternalLogStore.write}
     * (confirmed from its real source, not invented), minus the per-JVM
     * path lock that method also takes (not needed here: {@link
     * #commitWithRetry}'s own loop structure already serializes attempts
     * within this process) and minus the N-1 recovery check (hoisted out to
     * {@link #catchUpViaLock}, which runs earlier and covers this case more
     * correctly — see its javadoc).
     *
     * <b>Order, exactly matching the real algorithm:</b> write the commit
     * content to a temp file first (so a <i>different</i> process can
     * finish this commit later if this one crashes — it has no in-memory
     * copy of {@code content}) → write an incomplete external entry
     * (<b>the real mutual-exclusion point</b>: {@code overwrite=false}
     * throws on conflict, which propagates to {@link #commitWithRetry}'s
     * existing catch clause unchanged) → write the real final file (same-
     * process shortcut: reuse the in-memory {@code content} directly
     * instead of a literal temp-to-final object copy) → mark the entry
     * complete.
     */
    private void commitViaLock(LockProvider lock, String fileName, String content) throws IOException {
        String tablePath = tableLocation.raw();

        String tempPath = ".tmp/" + UUID.randomUUID() + ".json";
        if (!tableLocation.isRemote()) {
            Files.createDirectories(tableLocation.localPath().resolve("_delta_log").resolve(".tmp"));
        }
        storage.createCommitFile(tempPath, content);

        ExternalCommitEntry entry = new ExternalCommitEntry(tablePath, fileName, tempPath, false, null);
        lock.putEntry(entry, false); // the real mutual-exclusion point

        try {
            storage.createCommitFile(fileName, content);
        } catch (FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException alreadyLanded) {
            // Another process already completed this exact entry's copy
            // (same tempPath, so necessarily the same content) -- safe,
            // not a version conflict, matches the real algorithm's
            // "FileAlreadyExistsException means someone else already
            // copied these same contents" reasoning.
        }
        lock.putEntry(entry.asComplete(DEFAULT_LOCK_EXPIRY_SECONDS), true);
    }

    /**
     * Mirrors real Delta's {@code fixDeltaLog}: given a previous writer's
     * incomplete entry, finish what it started (the final file's content is
     * recoverable from {@code entry.tempPath()}, written by that writer
     * before it died or lost a race) and mark it complete, so this writer
     * can safely build on top of it. Bounded retries, matching the real
     * algorithm's shape.
     */
    private void recoverEntry(ExternalCommitEntry entry) {
        IOException lastError = null;
        for (int attempt = 0; attempt < MAX_RECOVERY_ATTEMPTS; attempt++) {
            try {
                try {
                    String content = readAll(storage.openReader(entry.tempPath()));
                    storage.createCommitFile(entry.fileName(), content);
                } catch (FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException alreadyThere) {
                    // the final file already exists -- fine, fall through to mark complete
                }
                lockProvider.putEntry(entry.asComplete(DEFAULT_LOCK_EXPIRY_SECONDS), true);
                return;
            } catch (IOException e) {
                lastError = e;
            }
        }
        throw new java.io.UncheckedIOException(
                "Failed to recover incomplete commit entry " + entry.fileName()
                        + " after " + MAX_RECOVERY_ATTEMPTS + " attempts", lastError);
    }

    private static String readAll(java.io.BufferedReader reader) throws IOException {
        try (reader) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
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
        TableLocation dataFileLocation = tableLocation.resolve(relativePath);

        if (!dataFileLocation.isRemote()) {
            // Remote (object storage) has no real directories to create;
            // local disk does.
            Files.createDirectories(dataFileLocation.localPath().getParent());
        }
        parquetWriter.write(dataFileLocation, batch);

        return new AddFile(
                relativePath.replace('\\', '/'),
                partitionValues,
                fileSize(dataFileLocation),
                System.currentTimeMillis(),
                dataChange,
                null);
    }

    private long fileSize(TableLocation location) throws IOException {
        if (!location.isRemote()) {
            return location.localPath().toFile().length();
        }
        return location.hadoopPath().getFileSystem(location.hadoopConf())
                .getFileStatus(location.hadoopPath()).getLen();
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
