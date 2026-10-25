package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The memtable/WAL layer in front of {@link CommitWriter} — build-order
 * step 2. Buffers incoming batches durably before they become a Parquet
 * file + commit, so frequent small writes don't turn into one tiny file
 * and one log entry per call. See {@code docs/labukdb-v2-architecture.md}'s
 * "Write path: it's an LSM tree" section for the design motivation.
 *
 * <b>Scope, stated plainly:</b> one instance buffers for one table AND
 * one fixed partition-value combination, given at construction — not
 * per-call like {@link CommitWriter#appendBatch}. This keeps {@link #flush()}
 * trivially atomic (one commit, succeeds or fails as a whole, no partial-
 * success state). A caller ingesting across multiple partitions manages
 * multiple instances — that routing belongs to a future ingest layer, not
 * here, the same way {@code CommitWriter} doesn't handle multi-table
 * routing itself.
 *
 * <b>WAL design:</b> one self-contained Arrow IPC file per {@link #append}
 * call (schema + one batch + end-of-stream marker, {@code fsync}'d before
 * the call returns) rather than one long-lived growing stream — a file is
 * atomically complete the moment it closes, so a crash mid-write never
 * leaves an unreadable half-written WAL segment. {@link #append} does
 * <b>not</b> take ownership of the caller's {@code VectorSchemaRoot} —
 * matches every existing call site's convention in this codebase — so the
 * WAL file itself, not an in-memory copy, is the actual source of
 * buffered data; {@link #flush()} reads it back fresh via
 * {@link ArrowStreamReader}. This unifies normal buffering and
 * crash-recovery replay into one code path, trading a little flush-time
 * I/O for materially simpler lifecycle management — deliberate, not an
 * oversight. A pure in-memory fast path is a reasonable future
 * optimization once this is proven correct.
 *
 * <b>Not in scope here:</b> reading buffered-but-unflushed data back into
 * query results ("read-your-writes") — deferred to land together with
 * {@code flight/DoPut}, same as {@code TableFormat}'s deferred wiring.
 * Background/scheduled time-based flush — no scheduler exists yet;
 * {@link #flush()} is available to call manually or from a future one.
 *
 * <b>The WAL always stays on local disk</b> — even when the table itself
 * is remote (S3/MinIO/ADLS, via {@link #WriteBuffer(TableLocation, Path,
 * Map, long, long, BufferAllocator)}). It's server-internal durability
 * staging for buffered-but-not-yet-flushed rows, never part of the Delta
 * protocol or visible to any other reader — redirecting it to remote
 * storage would trade away the whole latency/cost reason this class
 * exists, for zero extra correctness verification. {@code walRoot} is an
 * explicit, caller-supplied parameter rather than an auto-derived
 * default — keeps it trivially testable without touching a real
 * deployment's WAL directory.
 */
public final class WriteBuffer implements AutoCloseable {

    private final Map<String, String> partitionValues;
    private final long maxBufferedRows;
    private final long maxBufferedBytes;
    private final BufferAllocator allocator;
    private final Path walDir;
    private final CommitWriter commitWriter;

    private long bufferedRows;
    private long bufferedBytes;
    private long nextSequence;

    /** Local disk — unchanged behavior, WAL nested under the table's own root. */
    public WriteBuffer(Path tableRoot, Map<String, String> partitionValues,
                        long maxBufferedRows, long maxBufferedBytes,
                        BufferAllocator allocator) throws IOException {
        this(partitionValues, maxBufferedRows, maxBufferedBytes, allocator,
                tableRoot.resolve("_labukdb_wal").resolve(sanitizePartitionKey(partitionValues)),
                new CommitWriter(tableRoot));
    }

    /**
     * Local or remote (S3/MinIO/ADLS) table, via {@link TableLocation} —
     * what {@code LabukDbFlightProducer} uses so {@code DoPut} works the
     * same way against either. The WAL still lives under {@code walRoot}
     * on local disk regardless — see this class's javadoc for why.
     */
    public WriteBuffer(TableLocation location, Path walRoot, Map<String, String> partitionValues,
                        long maxBufferedRows, long maxBufferedBytes,
                        BufferAllocator allocator) throws IOException {
        this(partitionValues, maxBufferedRows, maxBufferedBytes, allocator,
                walRoot.resolve(sanitizeLocationKey(location)).resolve(sanitizePartitionKey(partitionValues)),
                CommitWriter.forLocation(location));
    }

    private WriteBuffer(Map<String, String> partitionValues, long maxBufferedRows, long maxBufferedBytes,
                         BufferAllocator allocator, Path walDir, CommitWriter commitWriter) throws IOException {
        this.partitionValues = partitionValues;
        this.maxBufferedRows = maxBufferedRows;
        this.maxBufferedBytes = maxBufferedBytes;
        this.allocator = allocator;
        this.walDir = walDir;
        this.commitWriter = commitWriter;

        recoverFromExistingWal();
    }

    /**
     * Writes one durable WAL file for {@code batch}, then auto-flushes if
     * either threshold is now exceeded. Does not close or take ownership
     * of {@code batch} — the caller's existing try-with-resources around
     * it keeps working exactly as it does with {@code CommitWriter.appendBatch}.
     */
    public void append(VectorSchemaRoot batch) throws IOException {
        Files.createDirectories(walDir);
        // Sequence-prefixed so lexicographic filename sort order is also
        // append order — plain UUIDs sort arbitrarily, which silently
        // scrambled row order across multiple WAL files at flush time.
        Path walFile = walDir.resolve(String.format("%016d-%s.wal", nextSequence++, UUID.randomUUID()));

        try (FileOutputStream fos = new FileOutputStream(walFile.toFile())) {
            FileChannel channel = fos.getChannel();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(batch, null, channel)) {
                writer.start();
                writer.writeBatch();
                writer.end();
                channel.force(true);
            }
        }

        bufferedRows += batch.getRowCount();
        bufferedBytes += Files.size(walFile);

        if (bufferedRows >= maxBufferedRows || bufferedBytes >= maxBufferedBytes) {
            flush();
        }
    }

    /**
     * Commits everything currently buffered as one new table version.
     * No-op (returns -1) if nothing is buffered. WAL files are only
     * deleted after the commit succeeds — if {@code CommitWriter.appendBatch}
     * throws, the WAL is left untouched so the data isn't lost.
     */
    public long flush() throws IOException {
        List<Path> walFiles = listWalFiles();
        if (walFiles.isEmpty()) {
            return -1;
        }

        VectorSchemaRoot combined = null;
        try {
            for (Path walFile : walFiles) {
                try (FileInputStream fis = new FileInputStream(walFile.toFile());
                     ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot loaded = reader.getVectorSchemaRoot();
                        if (combined == null) {
                            combined = VectorSchemaRoot.create(loaded.getSchema(), allocator);
                            // VectorSchemaRootAppender needs the target's offset/data
                            // buffers actually allocated (even at 0 rows) before it can
                            // append into them — a bare create() alone leaves zero-capacity
                            // buffers and fails with an IndexOutOfBoundsException on the
                            // first variable-width column it tries to append.
                            combined.allocateNew();
                        }
                        VectorSchemaRootAppender.append(combined, loaded);
                    }
                }
            }

            long newVersion = commitWriter.appendBatch(combined, partitionValues);

            for (Path walFile : walFiles) {
                Files.deleteIfExists(walFile);
            }
            bufferedRows = 0;
            bufferedBytes = 0;
            return newVersion;
        } finally {
            if (combined != null) {
                combined.close();
            }
        }
    }

    public long bufferedRowCount() {
        return bufferedRows;
    }

    /** Sum of on-disk WAL file sizes for what's currently buffered — exact, not approximated. */
    public long bufferedBytes() {
        return bufferedBytes;
    }

    /** Releases no allocator-backed resources of its own today (nothing is held in memory between calls) — present for API stability as that changes. Does NOT flush. */
    @Override
    public void close() {
        // Intentionally does not flush: buffered data already durably
        // survives via the WAL on disk, not via any in-memory state this
        // method would need to drain.
    }

    private void recoverFromExistingWal() throws IOException {
        for (Path walFile : listWalFiles()) {
            bufferedBytes += Files.size(walFile);
            try (FileInputStream fis = new FileInputStream(walFile.toFile());
                 ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
                while (reader.loadNextBatch()) {
                    bufferedRows += reader.getVectorSchemaRoot().getRowCount();
                }
            }

            long sequence = parseSequence(walFile);
            if (sequence >= nextSequence) {
                nextSequence = sequence + 1;
            }
        }
    }

    /** Sorted by filename, so append order survives across multiple WAL files — see {@link #append}. */
    private List<Path> listWalFiles() throws IOException {
        if (!Files.isDirectory(walDir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(walDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".wal"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    private static long parseSequence(Path walFile) {
        String name = walFile.getFileName().toString();
        return Long.parseLong(name.substring(0, name.indexOf('-')));
    }

    /**
     * {@code col1=val1,col2=val2}, keys sorted for determinism — a
     * restarted process must derive the exact same WAL directory for the
     * same partition combination regardless of map iteration order.
     * {@code _unpartitioned} for an empty map.
     */
    private static String sanitizePartitionKey(Map<String, String> partitionValues) {
        if (partitionValues.isEmpty()) {
            return "_unpartitioned";
        }
        Map<String, String> sorted = new TreeMap<>(partitionValues);
        List<String> parts = new ArrayList<>();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            parts.add(entry.getKey() + "=" + entry.getValue());
        }
        return String.join(",", parts);
    }

    /**
     * Turns a table location's raw URI/path into a filesystem-safe
     * directory name — character-replace, not a hash, so the WAL
     * directory for a given table stays human-readable/debuggable, same
     * style as {@link #sanitizePartitionKey}. Truncated defensively in
     * case an unusually long location would otherwise exceed a
     * filesystem's path-component length limit.
     */
    private static String sanitizeLocationKey(TableLocation location) {
        String sanitized = location.raw().replaceAll("[^a-zA-Z0-9.-]", "_");
        return sanitized.length() > 150 ? sanitized.substring(0, 150) : sanitized;
    }
}
