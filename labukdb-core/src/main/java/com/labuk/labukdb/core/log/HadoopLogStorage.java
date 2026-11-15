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
 * <b>Read path (listing/reading commits):</b> unverified against real
 * S3/MinIO/ADLS beyond what's described below — {@code SnapshotManagerTest}
 * proves this class produces identical results to {@link LocalNioLogStorage}
 * for the same local directory, which exercises Hadoop's local filesystem
 * dispatch, not a real remote endpoint.
 *
 * <b>Write path ({@code createCommitFile}):</b> verified against a real
 * MinIO cluster — see the atomicity breakdown below. S3 and ADLS remain
 * verified only via local-filesystem substitution ({@code HadoopLogStorageTest}),
 * not a real endpoint.
 *
 * <h2>{@code createCommitFile} — atomicity per backend</h2>
 * Implemented via the standard {@code FileSystem.create(path, overwrite=false)}
 * contract, which is <i>supposed</i> to throw {@link FileAlreadyExistsException}
 * if the file is already there. <b>Confirmed, not assumed, that this
 * guarantee does not reliably hold for every real backend under genuine
 * concurrency</b> — see the MinIO finding below, which is the actual
 * reason to distrust this primitive rather than theorize about it:
 * <ul>
 *   <li><b>S3 (native AWS):</b> Hadoop 3.4.2+ added a conditional PUT at
 *       stream close (<a href="https://issues.apache.org/jira/browse/HADOOP-19256">HADOOP-19256</a>,
 *       {@code fs.s3a.create.conditional.enabled}, on by default; this
 *       project pins Hadoop 3.4.3). Never exercised against a real AWS
 *       endpoint from this codebase.</li>
 *   <li><b>MinIO: confirmed NOT safe under real concurrency, on at least
 *       one real deployment.</b> A single <i>sequential</i> conflict
 *       (create, then a deliberately conflicting second create) does
 *       throw {@link FileAlreadyExistsException} correctly — that much was
 *       verified against a real cluster ({@code quay.io/minio/minio:latest},
 *       2026-10-20) and still holds. But a dedicated concurrency test later
 *       found the opposite result under genuine simultaneous writers: an
 *       isolated diagnostic — 8 threads calling {@code fs.create(path,
 *       overwrite=false)} on the identical key, at once, no retry logic
 *       involved — got 8 "successes" and zero conflicts detected. This
 *       deployment simply does not enforce the conditional-write header
 *       under concurrency, even though it honors it sequentially. Matches
 *       HADOOP-19256's own release notes, which warn conditional-write
 *       support "may not be supported on third party stores" and "can't
 *       be confirmed without testing" — this is exactly that case. See
 *       {@code CommitWriterConcurrentMinioIntegrationTest}, which fails on
 *       purpose as a standing proof of this gap. The fix is an opt-in
 *       out-of-band lock ({@link DynamoDbLockProvider}), not a
 *       retry-loop change — see {@code docs/ARCHITECTURE.md}'s
 *       "Concurrent writes" section.</li>
 *   <li><b>ADLS Gen2:</b> protected by Hadoop's create-without-overwrite-first
 *       pattern since Hadoop 3.3.0 (<a href="https://issues.apache.org/jira/browse/HADOOP-17215">HADOOP-17215</a>)
 *       — plausibly safe given ADLS's API has a real create-vs-overwrite
 *       distinction S3 lacked until 2024, but given the MinIO finding
 *       above, not safe to assume correct without the same kind of
 *       concurrent-writer test; not proven against a real ADLS endpoint
 *       from this codebase either way.</li>
 * </ul>
 * The practical conclusion: this project's remote multi-writer OCC
 * correctness guarantee currently depends on an assumption that's known to
 * be false for at least one common real deployment. Single-writer remote
 * use (the only case actually proven safe) is fine; concurrent remote
 * writers are not, until a real external lock is built.
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
    public Path hadoopPath(String fileName) {
        return new Path(logDir, fileName);
    }

    @Override
    public Configuration hadoopConf() {
        return fileSystem.getConf();
    }

    @Override
    public void writeTextFileOverwriting(String fileName, String content) throws IOException {
        try (FSDataOutputStream out = fileSystem.create(new Path(logDir, fileName), true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public String describe() {
        return logDir.toString();
    }
}
