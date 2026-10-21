package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

/**
 * What {@link SnapshotManager} and {@link CommitLogReader} need from the
 * {@code _delta_log} directory, abstracted away from any specific
 * filesystem. Two implementations:
 *
 * <ul>
 *   <li>{@link LocalNioLogStorage} — {@code java.nio.file}, the original
 *       implementation, behavior completely unchanged from before this
 *       abstraction existed.</li>
 *   <li>{@link HadoopLogStorage} — Hadoop's {@code FileSystem} API, which
 *       transparently supports S3/MinIO ({@code s3a://}), ADLS
 *       ({@code abfss://}), and others once the right connector jar and
 *       credentials config are present — the same mechanism {@code decode/}
 *       already partially relies on via {@code HadoopInputFile}.</li>
 * </ul>
 *
 * Introducing this did not change {@code SnapshotManager}'s local-path
 * behavior at all — the existing constructor still builds a
 * {@code LocalNioLogStorage} internally, same code path as before.
 */
interface LogStorage {

    /** Filenames only (not full paths), already filtered to the {@code NNNN...N.json} commit pattern. */
    List<String> listCommitFileNames() throws IOException;

    /** Filenames only, already filtered to ANY checkpoint file (single or multi-part). */
    List<String> listCheckpointFileNames() throws IOException;

    BufferedReader openReader(String fileName) throws IOException;

    long getModificationTime(String fileName) throws IOException;

    /** For {@link CheckpointReader} — a Parquet-mr {@code InputFile}, uniform across both backends. */
    org.apache.parquet.io.InputFile openParquetInputFile(String fileName) throws IOException;

    /**
     * Atomically creates a new commit file with the given content — must
     * fail (not silently overwrite) if a file with this name already
     * exists. This is the primitive {@link CommitWriter}'s single-writer
     * append relies on today, and that later multi-writer optimistic
     * concurrency control will rely on for conflict detection: two writers
     * racing for the same version number must have exactly one succeed.
     *
     * {@link LocalNioLogStorage} is genuinely atomic (local filesystem
     * {@code CREATE_NEW}). {@link HadoopLogStorage} implements the same
     * contract for S3/MinIO/ADLS, with differentiated, backend-specific
     * confidence levels documented on that class — see its javadoc before
     * relying on this for a specific remote backend.
     */
    void createCommitFile(String fileName, String content) throws IOException;

    /** For error messages. */
    String describe();

    /**
     * Resolves the {@code _delta_log} storage for a table location — the
     * one place that decides {@link LocalNioLogStorage} vs
     * {@link HadoopLogStorage}, reused by {@link CommitWriter} and
     * {@link TableCreator} instead of each re-implementing the dispatch.
     */
    static LogStorage forLocation(TableLocation location) throws IOException {
        return location.isRemote()
                ? new HadoopLogStorage(new org.apache.hadoop.fs.Path(location.hadoopPath(), "_delta_log"), location.hadoopConf())
                : new LocalNioLogStorage(location.localPath().resolve("_delta_log"));
    }
}
