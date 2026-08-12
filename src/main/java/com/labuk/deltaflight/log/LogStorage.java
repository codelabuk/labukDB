package com.labuk.deltaflight.log;

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

    /** For error messages. */
    String describe();
}
