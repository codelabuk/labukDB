package com.labuk.labukdb.core.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.RemoveFile;
import com.labuk.labukdb.core.log.action.SingleAction;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Appends one new data file to an existing Delta table: encodes a batch,
 * writes it under the table's Hive-style partition layout, and commits it
 * as a single {@code add} action in the next version's JSON commit file.
 *
 * <b>Scope, stated plainly — this is build-order step 1, nothing more:</b>
 * local disk only ({@link HadoopLogStorage#createCommitFile} throws rather
 * than pretending remote writes are safe — see its javadoc); appends to an
 * existing table only, no table creation; no concurrency control — two
 * {@code CommitWriter}s racing against the same table is undefined
 * behavior for now (one of {@link LogStorage#createCommitFile}'s atomic
 * creates will fail, but nothing here retries or detects the conflict
 * yet). See {@code docs/labukdb-v2-architecture.md}'s "Concurrent writes"
 * section for the planned multi-writer design this deliberately doesn't
 * implement yet.
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
        Snapshot current = snapshotManager.latest();
        long nextVersion = current.getVersion() + 1;

        AddFile addFile = encodeToNewFile(current, batch, partitionValues, true);

        String commitLine = mapper.writeValueAsString(SingleAction.ofAdd(addFile));
        storage.createCommitFile(CommitFileNames.jsonFileName(nextVersion), commitLine + "\n");

        return nextVersion;
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
     * @return the new table version
     */
    public long replaceFiles(Collection<AddFile> filesToRemove, VectorSchemaRoot mergedBatch,
                              Map<String, String> partitionValues) throws IOException {
        Snapshot current = snapshotManager.latest();
        long nextVersion = current.getVersion() + 1;

        AddFile addFile = encodeToNewFile(current, mergedBatch, partitionValues, false);

        StringBuilder commit = new StringBuilder();
        for (AddFile removed : filesToRemove) {
            RemoveFile removeFile = new RemoveFile(removed.getPath(), System.currentTimeMillis(), false);
            commit.append(mapper.writeValueAsString(SingleAction.ofRemove(removeFile))).append('\n');
        }
        commit.append(mapper.writeValueAsString(SingleAction.ofAdd(addFile))).append('\n');

        storage.createCommitFile(CommitFileNames.jsonFileName(nextVersion), commit.toString());
        return nextVersion;
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
