package com.labuk.deltaflight.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@code java.nio.file}-backed — identical behavior to SnapshotManager's original, pre-abstraction implementation. */
final class LocalNioLogStorage implements LogStorage {

    private final Path logDir;

    LocalNioLogStorage(Path logDir) {
        this.logDir = logDir;
    }

    @Override
    public List<String> listCommitFileNames() throws IOException {
        try (Stream<Path> stream = Files.list(logDir)) {
            return stream.map(p -> p.getFileName().toString())
                    .filter(CommitFileNames::isCommitJson)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<String> listCheckpointFileNames() throws IOException {
        try (Stream<Path> stream = Files.list(logDir)) {
            return stream.map(p -> p.getFileName().toString())
                    .filter(CommitFileNames::isSinglePartCheckpoint)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public BufferedReader openReader(String fileName) throws IOException {
        return Files.newBufferedReader(logDir.resolve(fileName));
    }

    @Override
    public long getModificationTime(String fileName) throws IOException {
        return Files.getLastModifiedTime(logDir.resolve(fileName)).toMillis();
    }

    @Override
    public org.apache.parquet.io.InputFile openParquetInputFile(String fileName) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(logDir.resolve(fileName).toString());
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(hadoopPath, new org.apache.hadoop.conf.Configuration());
    }

    @Override
    public String describe() {
        return logDir.toString();
    }
}
