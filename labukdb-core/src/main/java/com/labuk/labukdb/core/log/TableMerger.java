package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
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

    private final ParquetToArrowReader parquetReader = new ParquetToArrowReader();

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
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("merge() requires at least one key column");
        }

        SnapshotManager snapshotManager = SnapshotManager.forLocation(location);
        CommitWriter commitWriter = CommitWriter.forLocation(location);

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
        Map<List<Object>, Object[]> rows = new LinkedHashMap<>();
        Schema schema = null;

        if (!filesInPartition.isEmpty()) {
            VectorSchemaRoot combined = null;
            try {
                for (AddFile file : filesInPartition) {
                    TableLocation dataFile = location.resolve(file.getPath());
                    try (VectorSchemaRoot batch = parquetReader.read(dataFile, allocator, null, null, Map.of())) {
                        if (combined == null) {
                            combined = VectorSchemaRoot.create(batch.getSchema(), allocator);
                            combined.allocateNew();
                        }
                        VectorSchemaRootAppender.append(combined, batch);
                    }
                }
                schema = combined.getSchema();
                seedRows(rows, combined, keyColumns);
            } finally {
                if (combined != null) {
                    combined.close();
                }
            }
        }

        if (schema == null && inserts != null) {
            schema = inserts.getSchema();
        }
        if (schema == null && updates != null) {
            schema = updates.getSchema();
        }
        if (schema == null) {
            // Nothing to merge against and nothing to add -- an empty result is still
            // valid (e.g. a delete-only call against an already-empty partition).
            schema = (deletes != null) ? deletes.getSchema() : new Schema(List.of());
        }

        validateColumns(schema, keyColumns, "key column");

        if (deletes != null) {
            validateColumns(deletes.getSchema(), keyColumns, "key column");
            applyDeletes(rows, deletes, keyColumns);
        }
        if (updates != null) {
            // updates must carry the full physical row, not just the key --
            // a Debezium "after" image is always the complete row.
            validateColumns(updates.getSchema(), fieldNames(schema), "update column");
            applyUpsert(rows, updates, keyColumns, schema);
        }
        if (inserts != null) {
            validateColumns(inserts.getSchema(), fieldNames(schema), "insert column");
            applyUpsert(rows, inserts, keyColumns, schema);
        }

        return buildResult(schema, rows, allocator);
    }

    private List<String> fieldNames(Schema schema) {
        return schema.getFields().stream().map(Field::getName).toList();
    }

    private void validateColumns(Schema schema, List<String> required, String what) {
        List<String> missing = required.stream()
                .filter(name -> schema.findField(name) == null)
                .toList();
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("merge() batch is missing " + what + "(s): " + missing
                    + " -- schema has: " + fieldNames(schema));
        }
    }

    private void seedRows(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns) {
        validateColumns(batch.getSchema(), keyColumns, "key column");
        List<String> columns = fieldNames(batch.getSchema());
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.put(extractKey(batch, keyColumns, row), extractRow(batch, columns, row));
        }
    }

    private void applyUpsert(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns, Schema schema) {
        List<String> columns = fieldNames(schema);
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.put(extractKey(batch, keyColumns, row), extractRow(batch, columns, row));
        }
    }

    private void applyDeletes(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns) {
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.remove(extractKey(batch, keyColumns, row));
        }
    }

    private List<Object> extractKey(VectorSchemaRoot batch, List<String> keyColumns, int row) {
        List<Object> key = new ArrayList<>(keyColumns.size());
        for (String column : keyColumns) {
            key.add(getValue(batch.getVector(column), row));
        }
        return key;
    }

    private Object[] extractRow(VectorSchemaRoot batch, List<String> columns, int row) {
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = getValue(batch.getVector(columns.get(i)), row);
        }
        return values;
    }

    private VectorSchemaRoot buildResult(Schema schema, Map<List<Object>, Object[]> rows, BufferAllocator allocator) {
        VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
        result.allocateNew();
        int row = 0;
        for (Object[] values : rows.values()) {
            for (int col = 0; col < values.length; col++) {
                setValue(result.getVector(col), row, values[col]);
            }
            row++;
        }
        result.setRowCount(row);
        return result;
    }

    // Same small boxed-Object type switch AggregateOperator.getValue/setValue
    // already use — duplicated locally rather than extracted into a shared
    // utility; a third independent copy of a type-switch like this already
    // exists for Parquet encoding, consistent with this codebase's existing
    // tolerance for that duplication.

    private Object getValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }
        if (vector instanceof IntVector v) return v.get(index);
        if (vector instanceof BigIntVector v) return v.get(index);
        if (vector instanceof Float4Vector v) return v.get(index);
        if (vector instanceof Float8Vector v) return v.get(index);
        if (vector instanceof BitVector v) return v.get(index) != 0;
        if (vector instanceof VarCharVector v) return new String(v.get(index), StandardCharsets.UTF_8);
        if (vector instanceof TimeStampMicroVector v) return v.get(index);
        throw new UnsupportedOperationException("Unsupported vector type in TableMerger: " + vector.getClass());
    }

    private void setValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        if (vector instanceof IntVector v) v.setSafe(index, (Integer) value);
        else if (vector instanceof BigIntVector v) v.setSafe(index, ((Number) value).longValue());
        else if (vector instanceof Float4Vector v) v.setSafe(index, (Float) value);
        else if (vector instanceof Float8Vector v) v.setSafe(index, ((Number) value).doubleValue());
        else if (vector instanceof BitVector v) v.setSafe(index, ((Boolean) value) ? 1 : 0);
        else if (vector instanceof VarCharVector v) v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        else if (vector instanceof TimeStampMicroVector v) v.setSafe(index, ((Number) value).longValue());
        else throw new UnsupportedOperationException("Unsupported vector type in TableMerger: " + vector.getClass());
    }
}
