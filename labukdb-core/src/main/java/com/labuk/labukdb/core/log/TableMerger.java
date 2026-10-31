package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Upsert/MERGE by key — one table + partition per call, applying
 * {@code deletes} then {@code updates} then {@code inserts} against
 * whatever's currently in that partition, then committing the result as
 * a single atomic {@link CommitWriter#replaceFiles} call with
 * {@code dataChange=true} (unlike {@link Compactor}'s {@code false} —
 * row values genuinely change here, not just which file holds them).
 *
 * <b>The foundational piece a CDC source (e.g. Debezium) needs</b>:
 * {@link CommitWriter} alone can only blind-append or compact — nothing
 * applies an update or delete by key. This is deliberately NOT where a
 * key is defined as a persistent property of the table — {@code
 * keyColumns} is supplied fresh on every call, matching real Delta
 * {@code MERGE INTO}'s own behavior (its {@code ON} clause is always
 * specified at query time, never stored in table metadata). Storing it
 * in Delta's {@code metaData} action instead would be a private,
 * non-standard extension a real Spark/delta-rs/DuckDB client reading the
 * same table wouldn't understand — unlike Iceberg's real
 * {@code identifier-field-ids}, which IS persistent schema metadata, but
 * that's Iceberg's protocol, not the Delta protocol this class writes.
 *
 * <b>Keys stay in their partition</b> — no cross-partition search. A key
 * whose partition value changes over its lifetime isn't supported; that
 * would need an explicit delete in the old partition plus an insert in
 * the new one, orchestrated by the caller, not this method.
 *
 * <b>Per-row semantics, stated plainly, not silently invented:</b>
 * <ul>
 *   <li>Delete with no matching key — silent no-op (could mean the row
 *       was never captured here, not necessarily an error).</li>
 *   <li>Update with no matching key — still applied as an upsert
 *       (added), not rejected.</li>
 *   <li>Insert whose key collides with an existing/updated row —
 *       last-applied-wins (insert overwrites).</li>
 * </ul>
 *
 * <b>Residual limitations, named not hidden:</b> a narrow window remains
 * between this method's pre-commit freshness check and the actual
 * atomic write underneath it — closing that fully would need a
 * server-side "expected active file set" precondition on the commit
 * itself, out of scope here, the same category of accepted gap as this
 * project's existing "one orphan file per failed compaction attempt."
 * Every retry re-reads and re-decodes the entire partition's existing
 * data — the same cost profile {@link Compactor} already accepts (no
 * size heuristic). No schema validation beyond "does this named column
 * exist" — a batch with the right names but wrong types isn't caught.
 */
public final class TableMerger {

    private static final int MAX_RETRIES = 10;

    /**
     * Local disk — unchanged behavior from before {@link TableLocation} existed.
     *
     * @param inserts rows to add, keyed by {@code keyColumns}; {@code null} means none
     * @param updates rows that replace an existing key's full row; {@code null} means none
     * @param deletes rows (only {@code keyColumns} are read) to remove by key; {@code null} means none
     * @return the new table version
     */
    public long merge(Path tableRoot, Map<String, String> partitionValues, List<String> keyColumns,
                       VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                       BufferAllocator allocator) throws IOException {
        return merge(TableLocation.forLocal(tableRoot), partitionValues, keyColumns, inserts, updates, deletes, allocator);
    }

    /** Local or remote (S3/MinIO/ADLS), via {@link TableLocation} — same semantics as the {@code Path} overload. */
    public long merge(TableLocation location, Map<String, String> partitionValues, List<String> keyColumns,
                       VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                       BufferAllocator allocator) throws IOException {
        return merge(location, null, partitionValues, keyColumns, inserts, updates, deletes, allocator);
    }

    /**
     * Same as {@link #merge(TableLocation, Map, List, VectorSchemaRoot, VectorSchemaRoot, VectorSchemaRoot, BufferAllocator)},
     * but every commit is mediated through {@code lockProvider} — a real,
     * previously-missing gap closed here: {@link CommitWriter#forLocation(TableLocation, LockProvider)}
     * already lets a plain {@code appendBatch} opt into the DynamoDB-backed
     * mitigation for non-compliant object storage's conditional-create, but
     * this class (and {@link Compactor}) had no way to pass a lock through
     * at all — {@code merge}'s own {@code replaceFiles} commit was always
     * exposed to the same confirmed-broken race {@code appendBatch} already
     * has a fix for, with no way to opt in. {@code lockProvider == null}
     * keeps the existing no-lock behavior exactly as before.
     */
    public long merge(TableLocation location, LockProvider lockProvider, Map<String, String> partitionValues, List<String> keyColumns,
                       VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                       BufferAllocator allocator) throws IOException {
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("merge() requires at least one key column");
        }

        SnapshotManager snapshotManager = SnapshotManager.forLocation(location);
        CommitWriter commitWriter = lockProvider == null
                ? CommitWriter.forLocation(location)
                : CommitWriter.forLocation(location, lockProvider);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            Snapshot snapshot = snapshotManager.latest();
            List<AddFile> filesInPartition = snapshot.getActiveFiles().stream()
                    .filter(f -> f.getPartitionValues().equals(partitionValues))
                    .toList();

            VectorSchemaRoot merged = buildMergedBatch(location, filesInPartition, keyColumns,
                    inserts, updates, deletes, allocator);
            try {
                if (!stillFresh(snapshotManager, partitionValues, filesInPartition)) {
                    continue; // a concurrent writer touched this partition -- redo the whole merge against fresh data
                }
                return commitWriter.replaceFiles(filesInPartition, merged, partitionValues, true);
            } catch (ConcurrentModificationException e) {
                // one of filesInPartition was removed by a concurrent commit -- same redo, not a cheap retry
            } finally {
                merged.close();
            }
        }
        throw new IOException("Too much write contention on " + location
                + " -- merge failed to commit after " + MAX_RETRIES + " attempts");
    }

    /**
     * Catches what {@code replaceFiles}'s own internal conflict check
     * alone would miss: a concurrent writer having ADDED a new file to
     * this same partition, not just removed one of ours. Harmless for
     * {@link Compactor} (a missed concurrent add just isn't compacted
     * this round); not harmless here, since that new file's rows could
     * collide at the key level with what this merge is about to commit.
     */
    private boolean stillFresh(SnapshotManager snapshotManager, Map<String, String> partitionValues,
                                List<AddFile> expected) {
        Snapshot current = snapshotManager.latest();
        Set<String> currentPaths = current.getActiveFiles().stream()
                .filter(f -> f.getPartitionValues().equals(partitionValues))
                .map(AddFile::getPath).collect(Collectors.toSet());
        Set<String> expectedPaths = expected.stream().map(AddFile::getPath).collect(Collectors.toSet());
        return currentPaths.equals(expectedPaths);
    }

    private VectorSchemaRoot buildMergedBatch(TableLocation location, List<AddFile> filesInPartition, List<String> keyColumns,
                                               VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                                               BufferAllocator allocator) throws IOException {
        List<TableLocation> fileLocations = filesInPartition.stream()
                .map(file -> location.resolve(file.getPath()))
                .toList();
        VectorSchemaRoot combined = PartitionRowMerger.combineFiles(fileLocations, allocator);
        try {
            return PartitionRowMerger.merge(combined, keyColumns, inserts, updates, deletes, allocator);
        } finally {
            if (combined != null) {
                combined.close();
            }
        }
    }
}
