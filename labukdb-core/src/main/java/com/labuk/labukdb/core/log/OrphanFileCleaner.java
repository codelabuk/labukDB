package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.SingleAction;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Deletes real Parquet files that exist in table storage but were never
 * mentioned by <em>any</em> commit action, ever — not even as a {@code
 * remove} tombstone — the gap {@link Vacuum} genuinely doesn't cover,
 * since {@link Vacuum} is entirely log-driven (it only ever deletes a
 * path some commit already named in a {@code remove} action). A true
 * orphan has no such mention at all: the realistic cause is a writer
 * that wrote its Parquet file (every write path in this project writes
 * the data file before attempting its commit — see {@code
 * CommitWriter#encodeToNewFile}) and then never successfully committed
 * (a crash, an exhausted OCC retry loop, a process killed mid-write).
 *
 * <b>Scans the full commit history for "known" paths</b>, both {@code
 * add} and {@code remove}, the same full-history approach {@link Vacuum}
 * already uses for its own purpose — a file mentioned anywhere, even as
 * an old tombstone still within {@link Vacuum}'s own retention window, is
 * not an orphan; it's {@link Vacuum}'s job, not this class's.
 *
 * <b>The one safety invariant that actually matters:</b> a file younger
 * than the retention period is never deleted, regardless of whether it's
 * mentioned anywhere — exactly the same reasoning real Delta's own
 * {@code VACUUM} retention window exists for: a file that's genuinely
 * mid-write by a concurrent, not-yet-committed transaction must never be
 * mistaken for an orphan just because nothing has committed it yet.
 *
 * <b>Only files ending in {@code .parquet} are ever considered</b> —
 * deliberately conservative, so an unrelated marker or checksum file
 * some other tool left in the table directory is never a deletion
 * candidate.
 */
public final class OrphanFileCleaner {

    /** Matches {@link Vacuum#DEFAULT_RETENTION} — same safety reasoning, not a coincidence. */
    public static final Duration DEFAULT_RETENTION = Vacuum.DEFAULT_RETENTION;

    public List<String> clean(TableLocation location) throws IOException {
        return clean(location, DEFAULT_RETENTION);
    }

    public List<String> clean(TableLocation location, Duration retentionPeriod) throws IOException {
        long cutoffMillis = System.currentTimeMillis() - retentionPeriod.toMillis();

        Set<String> knownPaths = collectEveryPathEverMentioned(location);
        List<FileEntry> actualFiles = listDataFiles(location);

        List<String> deleted = new ArrayList<>();
        for (FileEntry file : actualFiles) {
            if (knownPaths.contains(file.relativePath())) {
                continue; // mentioned somewhere in the log -- active, or Vacuum's own job, not an orphan
            }
            if (file.modificationTime() >= cutoffMillis) {
                continue; // too recent -- could be an in-flight writer that hasn't committed yet
            }
            if (deleteDataFile(location.resolve(file.relativePath()))) {
                deleted.add(file.relativePath());
            }
        }
        return deleted;
    }

    /** Every {@code add}/{@code remove} path mentioned anywhere in the full commit history — same full-history scan {@link Vacuum} already does, for a different purpose. */
    private Set<String> collectEveryPathEverMentioned(TableLocation location) throws IOException {
        LogStorage storage = LogStorage.forLocation(location);
        CommitLogReader commitLogReader = new CommitLogReader();

        Set<String> known = new LinkedHashSet<>();
        for (String fileName : storage.listCommitFileNames()) {
            try (BufferedReader reader = storage.openReader(fileName)) {
                for (SingleAction action : commitLogReader.readFrom(reader, fileName)) {
                    if (action.getAdd() != null) {
                        known.add(action.getAdd().getPath());
                    }
                    if (action.getRemove() != null) {
                        known.add(action.getRemove().getPath());
                    }
                }
            }
        }
        return known;
    }

    /** A real file's path, relative to the table root, and its last-modified time (for the retention check). */
    private record FileEntry(String relativePath, long modificationTime) {
    }

    private List<FileEntry> listDataFiles(TableLocation location) throws IOException {
        return location.isRemote() ? listRemoteDataFiles(location) : listLocalDataFiles(location);
    }

    private List<FileEntry> listLocalDataFiles(TableLocation location) throws IOException {
        Path root = location.localPath();
        Path deltaLog = root.resolve("_delta_log");
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> !p.startsWith(deltaLog))
                    .filter(p -> p.getFileName().toString().endsWith(".parquet"))
                    .map(p -> toFileEntry(root, p))
                    .toList();
        }
    }

    private FileEntry toFileEntry(Path root, Path file) {
        try {
            String relativePath = root.relativize(file).toString().replace('\\', '/');
            long modificationTime = Files.getLastModifiedTime(file).toMillis();
            return new FileEntry(relativePath, modificationTime);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<FileEntry> listRemoteDataFiles(TableLocation location) throws IOException {
        org.apache.hadoop.fs.Path root = location.hadoopPath();
        org.apache.hadoop.fs.FileSystem fileSystem = root.getFileSystem(location.hadoopConf());
        String rootPrefix = root.toString();
        if (!rootPrefix.endsWith("/")) {
            rootPrefix = rootPrefix + "/";
        }
        String deltaLogPrefix = rootPrefix + "_delta_log/";

        List<FileEntry> files = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(root, true);
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            String fullPath = status.getPath().toString();
            if (fullPath.startsWith(deltaLogPrefix) || !fullPath.endsWith(".parquet")) {
                continue;
            }
            String relativePath = fullPath.startsWith(rootPrefix) ? fullPath.substring(rootPrefix.length()) : fullPath;
            files.add(new FileEntry(relativePath, status.getModificationTime()));
        }
        return files;
    }

    /** @return true if a file was actually deleted (mirrors {@code Files.deleteIfExists}'s return contract) -- same primitive {@link Vacuum} already uses. */
    private boolean deleteDataFile(TableLocation dataFile) throws IOException {
        if (!dataFile.isRemote()) {
            return Files.deleteIfExists(dataFile.localPath());
        }
        return dataFile.hadoopPath().getFileSystem(dataFile.hadoopConf()).delete(dataFile.hadoopPath(), false);
    }
}
