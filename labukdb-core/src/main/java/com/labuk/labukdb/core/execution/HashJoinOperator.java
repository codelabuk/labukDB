package com.labuk.labukdb.core.execution;

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Two-table {@code INNER} equi-join (SQL engine Phase 3, increment 1) —
 * both sides already fully decoded into memory (a join inherently needs
 * to see every row of at least the build side, the same assumption
 * {@code GROUP BY}/{@code ORDER BY} already make via {@code
 * CombinedQueryExecutor}). Classic hash build/probe: hashes {@code
 * right} by its key tuple, then probes with each {@code left} row —
 * {@code right} is always the build side in this increment, no
 * cost-based smaller-side selection yet (a stated simplification, not a
 * correctness issue). A duplicate key on the build side correctly
 * produces multiple emitted rows (true fan-out), and any row with a
 * {@code null} key component never matches anything, on either side —
 * standard SQL join-key null semantics.
 *
 * Output columns are every left column followed by every right column,
 * each renamed {@code "<alias>.<originalName>"} — the one place a SQL-
 * level table alias becomes a physical Arrow field name, purely so two
 * same-named columns from each side (e.g. both tables having an
 * {@code id} column) don't collide in the combined output.
 */
public final class HashJoinOperator {

    public VectorSchemaRoot innerJoin(VectorSchemaRoot left, List<String> leftKeys, String leftAlias,
                                       VectorSchemaRoot right, List<String> rightKeys, String rightAlias,
                                       BufferAllocator allocator) {
        if (leftKeys.isEmpty() || leftKeys.size() != rightKeys.size()) {
            throw new IllegalArgumentException("Join requires at least one key pair, with equal counts on each side");
        }

        List<FieldVector> rightKeyVectors = rightKeys.stream().map(right::getVector).toList();
        Map<List<Object>, List<Integer>> buildIndex = new LinkedHashMap<>();
        for (int row = 0; row < right.getRowCount(); row++) {
            List<Object> key = keyTuple(rightKeyVectors, row);
            if (key == null) {
                continue; // a null key component never matches -- standard SQL join semantics
            }
            buildIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        List<Field> outputFields = new ArrayList<>();
        for (Field field : left.getSchema().getFields()) {
            outputFields.add(renamed(field, leftAlias));
        }
        for (Field field : right.getSchema().getFields()) {
            outputFields.add(renamed(field, rightAlias));
        }
        VectorSchemaRoot result = VectorSchemaRoot.create(new Schema(outputFields), allocator);
        result.allocateNew();

        List<FieldVector> leftKeyVectors = leftKeys.stream().map(left::getVector).toList();
        List<FieldVector> leftAllVectors = left.getFieldVectors();
        List<FieldVector> rightAllVectors = right.getFieldVectors();

        int outRow = 0;
        for (int l = 0; l < left.getRowCount(); l++) {
            List<Object> key = keyTuple(leftKeyVectors, l);
            if (key == null) {
                continue;
            }
            List<Integer> matches = buildIndex.get(key);
            if (matches == null) {
                continue;
            }
            for (int r : matches) {
                int col = 0;
                for (FieldVector v : leftAllVectors) {
                    setValue(result.getVector(col++), outRow, getValue(v, l));
                }
                for (FieldVector v : rightAllVectors) {
                    setValue(result.getVector(col++), outRow, getValue(v, r));
                }
                outRow++;
            }
        }
        result.setRowCount(outRow);
        return result;
    }

    private List<Object> keyTuple(List<FieldVector> keyVectors, int row) {
        List<Object> key = new ArrayList<>(keyVectors.size());
        for (FieldVector v : keyVectors) {
            if (v.isNull(row)) {
                return null;
            }
            key.add(getValue(v, row));
        }
        return key;
    }

    private Field renamed(Field field, String alias) {
        return new Field(alias + "." + field.getName(), field.getFieldType(), field.getChildren());
    }

    // Same boxed-value get/set pattern AggregateOperator already uses -- two
    // independent, small occurrences don't yet justify a shared utility.
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
        throw new UnsupportedOperationException("Unsupported vector type in execution/: " + vector.getClass());
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
        else throw new UnsupportedOperationException("Unsupported vector type in execution/: " + vector.getClass());
    }
}
