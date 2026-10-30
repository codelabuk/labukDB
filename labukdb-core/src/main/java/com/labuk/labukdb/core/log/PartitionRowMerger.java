package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The format-agnostic half of upsert/MERGE-by-key — extracted from {@link
 * TableMerger} (which keeps the Delta-specific halves: resolving "files
 * currently in this partition" from an {@code AddFile} list, and
 * committing via {@link CommitWriter#replaceFiles}) so Iceberg's own
 * merger (a second, later call site) applies the exact same key-based
 * semantics rather than risk a second, independently-evolving copy of
 * nontrivial business rules — see {@link #merge}'s own javadoc for what
 * those rules are.
 *
 * <b>Deliberately not extracted further into per-type-switch sharing
 * with {@code AggregateOperator}/{@code TableMerger}'s own former
 * copy</b> — this project already tolerates that specific duplication
 * (see {@code TableMerger}'s own historical comment); what matters here
 * is the one copy of the actual merge algorithm, not the small
 * getValue/setValue plumbing underneath it.
 */
public final class PartitionRowMerger {

    private PartitionRowMerger() {
    }

    /**
     * Decodes and combines every file in {@code dataFiles} into one
     * {@link VectorSchemaRoot} — the read side of a merge, shared because
     * it's identical regardless of which table format resolved the file
     * list. Returns {@code null} for an empty list (an empty-but-real
     * partition is different from "nothing to combine" only at the
     * caller's decision level, not here). Caller owns and must close the
     * returned root.
     */
    public static VectorSchemaRoot combineFiles(List<TableLocation> dataFiles, BufferAllocator allocator) throws IOException {
        if (dataFiles.isEmpty()) {
            return null;
        }
        ParquetToArrowReader reader = new ParquetToArrowReader();
        VectorSchemaRoot combined = null;
        try {
            for (TableLocation dataFile : dataFiles) {
                try (VectorSchemaRoot batch = reader.read(dataFile, allocator, null, null, Map.of())) {
                    if (combined == null) {
                        combined = VectorSchemaRoot.create(batch.getSchema(), allocator);
                        combined.allocateNew();
                    }
                    VectorSchemaRootAppender.append(combined, batch);
                }
            }
            return combined;
        } catch (IOException | RuntimeException e) {
            if (combined != null) {
                combined.close();
            }
            throw e;
        }
    }

    /**
     * Applies {@code deletes} then {@code updates} then {@code inserts}
     * against {@code existingCombined} (the partition's current rows,
     * already decoded via {@link #combineFiles} — {@code null} means the
     * partition was empty or didn't exist yet), keyed by {@code
     * keyColumns}.
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
     * {@code existingCombined} is read-only here — this never closes it;
     * the caller (who built it via {@link #combineFiles}) owns it.
     */
    public static VectorSchemaRoot merge(VectorSchemaRoot existingCombined, List<String> keyColumns,
                                          VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                                          BufferAllocator allocator) {
        Map<List<Object>, Object[]> rows = new LinkedHashMap<>();
        Schema schema = null;

        if (existingCombined != null) {
            schema = existingCombined.getSchema();
            seedRows(rows, existingCombined, keyColumns);
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

    private static List<String> fieldNames(Schema schema) {
        return schema.getFields().stream().map(Field::getName).toList();
    }

    private static void validateColumns(Schema schema, List<String> required, String what) {
        List<String> missing = required.stream()
                .filter(name -> schema.findField(name) == null)
                .toList();
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("merge() batch is missing " + what + "(s): " + missing
                    + " -- schema has: " + fieldNames(schema));
        }
    }

    private static void seedRows(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns) {
        validateColumns(batch.getSchema(), keyColumns, "key column");
        List<String> columns = fieldNames(batch.getSchema());
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.put(extractKey(batch, keyColumns, row), extractRow(batch, columns, row));
        }
    }

    private static void applyUpsert(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns, Schema schema) {
        List<String> columns = fieldNames(schema);
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.put(extractKey(batch, keyColumns, row), extractRow(batch, columns, row));
        }
    }

    private static void applyDeletes(Map<List<Object>, Object[]> rows, VectorSchemaRoot batch, List<String> keyColumns) {
        for (int row = 0; row < batch.getRowCount(); row++) {
            rows.remove(extractKey(batch, keyColumns, row));
        }
    }

    private static List<Object> extractKey(VectorSchemaRoot batch, List<String> keyColumns, int row) {
        List<Object> key = new ArrayList<>(keyColumns.size());
        for (String column : keyColumns) {
            key.add(getValue(batch.getVector(column), row));
        }
        return key;
    }

    private static Object[] extractRow(VectorSchemaRoot batch, List<String> columns, int row) {
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = getValue(batch.getVector(columns.get(i)), row);
        }
        return values;
    }

    private static VectorSchemaRoot buildResult(Schema schema, Map<List<Object>, Object[]> rows, BufferAllocator allocator) {
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

    private static Object getValue(FieldVector vector, int index) {
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
        throw new UnsupportedOperationException("Unsupported vector type in PartitionRowMerger: " + vector.getClass());
    }

    private static void setValue(FieldVector vector, int index, Object value) {
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
        else throw new UnsupportedOperationException("Unsupported vector type in PartitionRowMerger: " + vector.getClass());
    }
}
