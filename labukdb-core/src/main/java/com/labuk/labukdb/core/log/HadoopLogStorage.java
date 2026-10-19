package com.labuk.labukdb.core.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Hadoop {@code FileSystem}-backed — new capability, not a replacement for
 * {@link LocalNioLogStorage}. {@code FileSystem.get(URI, Configuration)}
 * dispatches on URI scheme: a plain local path with no scheme resolves to
 * Hadoop's own local filesystem implementation (behaving equivalently to
 * direct {@code java.nio.file} access), while {@code s3a://}, {@code abfss://},
 * etc. resolve to the matching connector — IF that connector's jar
 * ({@code hadoop-aws} for S3/MinIO, {@code hadoop-azure} for ADLS) and the
 * right {@code Configuration} entries (endpoint, credentials) are present.
 * This class doesn't hardcode any of that config — callers build the
 * {@code Configuration} however fits their deployment (env vars, a
 * properties file, whatever), and hand it in.
 *
 * <b>Unverified against real S3/MinIO/ADLS</b> — this environment has no
 * way to test that. What's tested (see {@code SnapshotManagerTest}) is
 * that this class produces identical results to {@link LocalNioLogStorage}
 * when both point at the same local directory, since Hadoop's local
 * filesystem implementation should behave equivalently for local paths —
 * that's the part actually exercised, not a real S3 endpoint.
 *
 * <h2>{@code createCommitFile} — atomicity per backend</h2>
 * Implemented via the standard {@code FileSystem.create(path, overwrite=false)}
 * contract, which throws {@link FileAlreadyExistsException} if the file is
 * already there. Whether that's genuinely race-free, not just
 * check-then-act, differs by backend — confirmed via Hadoop's own issue
 * tracker, not assumed:
 * <ul>
 *   <li><b>S3 (native AWS):</b> atomic as of Hadoop 3.4.2+'s conditional
 *       PUT at stream close (<a href="https://issues.apache.org/jira/browse/HADOOP-19256">HADOOP-19256</a>,
 *       {@code fs.s3a.create.conditional.enabled}, on by default). This
 *       project pins Hadoop 3.4.3.</li>
 *   <li><b>MinIO:</b> same code path as S3, but actual atomicity depends
 *       on whether that specific MinIO deployment honors the conditional
 *       header — <b>not guaranteed by this project</b>; verify against
 *       your own deployment before relying on it.</li>
 *   <li><b>ADLS Gen2:</b> protected by Hadoop's create-without-overwrite-first
 *       pattern since Hadoop 3.3.0 (<a href="https://issues.apache.org/jira/browse/HADOOP-17215">HADOOP-17215</a>)
 *       — plausibly safe given ADLS's API has a real create-vs-overwrite
 *       distinction S3 lacked until 2024, but not proven against a real
 *       ADLS endpoint from this codebase.</li>
 * </ul>
 * None of the three have been exercised against real cloud infrastructure
 * from this environment — what's tested here is the contract itself
 * (create-then-recreate-fails) against Hadoop's local filesystem
 * implementation, the same substitution {@code HadoopBackedSnapshotManagerTest}
 * already uses for the read path.
 */
final class HadoopLogStorage implements LogStorage {

    private final FileSystem fileSystem;
    private final Path logDir;

    HadoopLogStorage(Path logDir, Configuration conf) throws IOException {
        this.logDir = logDir;
        this.fileSystem = logDir.getFileSystem(conf);
    }

    @Override
    public List<String> listCommitFileNames() throws IOException {
        return listMatching(CommitFileNames::isCommitJson);
    }

    @Override
    public List<String> listCheckpointFileNames() throws IOException {
        return listMatching(CommitFileNames::isAnyCheckpoint);
    }

    private List<String> listMatching(java.util.function.Predicate<String> filter) throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(logDir);
        List<String> names = new ArrayList<>();
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (filter.test(name)) {
                names.add(name);
            }
        }
        return names;
    }

    @Override
    public BufferedReader openReader(String fileName) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(logDir, fileName));
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    @Override
    public long getModificationTime(String fileName) throws IOException {
        return fileSystem.getFileStatus(new Path(logDir, fileName)).getModificationTime();
    }

    @Override
    public org.apache.parquet.io.InputFile openParquetInputFile(String fileName) throws IOException {
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new Path(logDir, fileName), fileSystem.getConf());
    }

    @Override
    public void createCommitFile(String fileName, String content) throws IOException {
        try (FSDataOutputStream out = fileSystem.create(new Path(logDir, fileName), false)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public String describe() {
        return logDir.toString();
    }
}
