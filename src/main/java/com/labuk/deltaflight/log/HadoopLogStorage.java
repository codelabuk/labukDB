package com.labuk.deltaflight.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
        return listMatching(CommitFileNames::isSinglePartCheckpoint);
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
    public String describe() {
        return logDir.toString();
    }
}
