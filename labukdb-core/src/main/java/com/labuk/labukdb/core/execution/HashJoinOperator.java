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

        List<FieldVector> leftKeyVectors = leftKeys.stream().map(left::getVector).toList();
        List<FieldVector> rightKeyVectors = rightKeys.stream().map(right::getVector).toList();

        // Single primitive-typed key column on both sides -> hash/compare as
        // a raw long, no per-row boxed List<Object> composite key (just one
        // boxed Long per *distinct key*, same tradeoff AggregateOperator's
        // fast path already makes -- java.util.HashMap's own internals beat
        // a hand-rolled open-addressing table at the row counts measured
        // here, so this reuses a standard Map rather than a bespoke one;
        // see docs/ARCHITECTURE.md for the benchmark that showed
        // this). Composite keys and VARCHAR keys fall through to the boxed
        // List<Object> path below unchanged. Requiring the *same* vector
        // class on both sides mirrors the boxed path's own existing
        // behavior: getValue() boxes IntVector to Integer and BigIntVector
        // to Long, which never .equals() each other either -- so this fast
        // path introduces no new cross-type matching behavior, just a
        // faster version of what already matches.
        boolean fastPath = leftKeyVectors.size() == 1
                && isLongEncodable(leftKeyVectors.get(0))
                && leftKeyVectors.get(0).getClass() == rightKeyVectors.get(0).getClass();

        Map<Long, List<Integer>> longIndex = null;
        Map<List<Object>, List<Integer>> boxedIndex = null;
        if (fastPath) {
            FieldVector rightKey = rightKeyVectors.get(0);
            longIndex = new LinkedHashMap<>();
            for (int row = 0; row < right.getRowCount(); row++) {
                if (rightKey.isNull(row)) {
                    continue; // a null key component never matches -- standard SQL join semantics
                }
                longIndex.computeIfAbsent(encodeLong(rightKey, row), k -> new ArrayList<>()).add(row);
            }
        } else {
            boxedIndex = new LinkedHashMap<>();
            for (int row = 0; row < right.getRowCount(); row++) {
                List<Object> key = keyTuple(rightKeyVectors, row);
                if (key == null) {
                    continue;
                }
                boxedIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
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

        List<FieldVector> leftAllVectors = left.getFieldVectors();
        List<FieldVector> rightAllVectors = right.getFieldVectors();

        int outRow = 0;
        if (fastPath) {
            FieldVector leftKey = leftKeyVectors.get(0);
            for (int l = 0; l < left.getRowCount(); l++) {
                if (leftKey.isNull(l)) {
                    continue;
                }
                List<Integer> matches = longIndex.get(encodeLong(leftKey, l));
                outRow = emitMatches(matches, l, leftAllVectors, rightAllVectors, result, outRow);
            }
        } else {
            for (int l = 0; l < left.getRowCount(); l++) {
                List<Object> key = keyTuple(leftKeyVectors, l);
                List<Integer> matches = key == null ? null : boxedIndex.get(key);
                outRow = emitMatches(matches, l, leftAllVectors, rightAllVectors, result, outRow);
            }
        }
        result.setRowCount(outRow);
        return result;
    }

    private static int emitMatches(List<Integer> matches, int leftRow, List<FieldVector> leftAllVectors,
                                     List<FieldVector> rightAllVectors, VectorSchemaRoot result, int outRow) {
        if (matches == null) {
            return outRow;
        }
        for (int r : matches) {
            int col = 0;
            for (FieldVector v : leftAllVectors) {
                copyValue(v, leftRow, result.getVector(col++), outRow);
            }
            for (FieldVector v : rightAllVectors) {
                copyValue(v, r, result.getVector(col++), outRow);
            }
            outRow++;
        }
        return outRow;
    }

    private static boolean isLongEncodable(FieldVector v) {
        return v instanceof IntVector || v instanceof BigIntVector || v instanceof BitVector
                || v instanceof Float4Vector || v instanceof Float8Vector || v instanceof TimeStampMicroVector;
    }

    /** Must only be called on a vector {@link #isLongEncodable}; widens/bit-casts so equal values hash/compare equal. */
    private static long encodeLong(FieldVector v, int row) {
        if (v instanceof IntVector iv) return iv.get(row);
        if (v instanceof BigIntVector bv) return bv.get(row);
        if (v instanceof BitVector bv) return bv.get(row) != 0 ? 1L : 0L;
        if (v instanceof Float4Vector fv) return Double.doubleToLongBits((double) fv.get(row));
        if (v instanceof Float8Vector fv) return Double.doubleToLongBits(fv.get(row));
        if (v instanceof TimeStampMicroVector tv) return tv.get(row);
        throw new IllegalStateException("Not long-encodable: " + v.getClass());
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

    /**
     * Primitive-to-primitive copy, no {@code Object} ever created -- this
     * runs once per column per *matched pair*, the highest-volume site in
     * the whole operator (a duplicate build-side key multiplies it by the
     * fan-out), so boxing here is the first thing worth removing. {@code
     * getValue} below still exists for the key-extraction path, which only
     * avoids per-row composite-key boxing for a single primitive-typed key
     * column (see the {@code fastPath} branch in {@link #innerJoin}) --
     * composite and VARCHAR keys still need it.
     */
    private static void copyValue(FieldVector src, int srcIdx, FieldVector dest, int destIdx) {
        if (src.isNull(srcIdx)) {
            dest.setNull(destIdx);
            return;
        }
        if (src instanceof IntVector s) ((IntVector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof BigIntVector s) ((BigIntVector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof Float4Vector s) ((Float4Vector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof Float8Vector s) ((Float8Vector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof BitVector s) ((BitVector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof VarCharVector s) ((VarCharVector) dest).setSafe(destIdx, s.get(srcIdx));
        else if (src instanceof TimeStampMicroVector s) ((TimeStampMicroVector) dest).setSafe(destIdx, s.get(srcIdx));
        else throw new UnsupportedOperationException("Unsupported vector type in execution/: " + src.getClass());
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
}
