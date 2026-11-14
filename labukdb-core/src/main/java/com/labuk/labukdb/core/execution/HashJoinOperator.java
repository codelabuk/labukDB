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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hash-based equi-join — {@code INNER}, {@code LEFT}, {@code RIGHT},
 * {@code FULL}, {@code SEMI}, and {@code ANTI} — both sides already
 * fully decoded into memory (a join inherently needs to see every row
 * of at least the build side, the same assumption {@code GROUP BY}/
 * {@code ORDER BY} already make via {@code CombinedQueryExecutor}).
 *
 * <b>{@code SEMI}/{@code ANTI}</b> — how SQL engine Phase 4's decorrelated
 * {@code IN}/{@code EXISTS}/{@code NOT IN}/{@code NOT EXISTS} subqueries
 * execute (see {@code core.query.SqlQueryTranslator}). Neither ever
 * fans out, and neither includes the build (subquery) side's columns in
 * the output at all — the build index is still the same one every other
 * type uses, but the probe loop emits the probe row at most once
 * ({@code SEMI}: if it has any match; {@code ANTI}: if it has none),
 * via {@link #emitProbeOnly}, not {@link #emitMatchedPair}.
 *
 * <b>{@code RIGHT} is not a real algorithm here</b> — {@code RIGHT(left,
 * right)} is exactly {@code LEFT(right, left)} with every argument
 * swapped (roots, keys, aliases), so {@link #join} delegates to the
 * {@code LEFT} case rather than duplicating it. Output column order
 * becomes right-then-left for that one call, which is safe: every
 * caller looks columns up by their alias-qualified name, never by
 * position (see {@code core.query.SqlQueryTranslator}'s own javadoc).
 *
 * <b>{@code FULL} is {@code LEFT}'s unmatched-left-row pass plus a
 * second pass for unmatched build-side rows</b> — a {@link BitSet}
 * tracks which build-side rows matched at least once during probing;
 * anything left unset gets emitted afterward with the probe side's
 * columns null. {@code INNER} keeps unmatched rows on either side
 * dropped, same as before.
 *
 * <b>Build side:</b> hashes one side by its key tuple, then probes with
 * the other. For {@code LEFT}/{@code FULL}, the non-preserved side is
 * always the build side (unchanged, fixed convention — the correctness-
 * critical null-padding logic stays simple, not entangled with a
 * performance heuristic). For {@code INNER} only, {@code buildFromLeft}
 * lets the caller pick the smaller side as the build side (a byte-size
 * heuristic — see {@code core.cli.QueryCommand} — not real cost-based
 * optimization); ignored for every other join type. A duplicate key on
 * the build side correctly produces multiple emitted rows (true
 * fan-out), and any row with a {@code null} key component never matches
 * anything, on either side — standard SQL join-key null semantics.
 *
 * Output columns are every left column followed by every right column
 * (by declared parameter order, regardless of which side was the build
 * side), each renamed {@code "<alias>.<originalName>"} — the one place
 * a SQL-level table alias becomes a physical Arrow field name, purely
 * so two same-named columns from each side don't collide in the
 * combined output. {@code leftAlias}/{@code rightAlias} may each be
 * {@code null} instead, meaning that side's columns are already in
 * their final, alias-qualified form and must be passed through
 * unrenamed — needed for a 3+-way join's later steps, whose "left" is
 * itself a previous call's already-renamed combined output (see {@code
 * core.logical.LogicalPlanBuilder}'s own javadoc for the bare-vs-
 * qualified key-name convention this pairs with).
 */
public final class HashJoinOperator {

    public VectorSchemaRoot join(JoinType type,
                                  VectorSchemaRoot left, List<String> leftKeys, String leftAlias,
                                  VectorSchemaRoot right, List<String> rightKeys, String rightAlias,
                                  boolean buildFromLeft, BufferAllocator allocator) {
        if (type == JoinType.RIGHT) {
            return join(JoinType.LEFT, right, rightKeys, rightAlias, left, leftKeys, leftAlias, false, allocator);
        }
        if (leftKeys.isEmpty() || leftKeys.size() != rightKeys.size()) {
            throw new IllegalArgumentException("Join requires at least one key pair, with equal counts on each side");
        }

        // LEFT/FULL always build from the non-preserved side (right) --
        // fixed, not cost-based. INNER is symmetric (no preserved side),
        // so buildFromLeft may pick left instead.
        boolean buildIsRight = !(type == JoinType.INNER && buildFromLeft);

        VectorSchemaRoot buildRoot = buildIsRight ? right : left;
        VectorSchemaRoot probeRoot = buildIsRight ? left : right;
        List<String> buildKeyNames = buildIsRight ? rightKeys : leftKeys;
        List<String> probeKeyNames = buildIsRight ? leftKeys : rightKeys;

        List<FieldVector> buildKeyVectors = buildKeyNames.stream().map(buildRoot::getVector).toList();
        List<FieldVector> probeKeyVectors = probeKeyNames.stream().map(probeRoot::getVector).toList();

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
        boolean fastPath = buildKeyVectors.size() == 1
                && isLongEncodable(buildKeyVectors.get(0))
                && buildKeyVectors.get(0).getClass() == probeKeyVectors.get(0).getClass();

        Map<Long, List<Integer>> longIndex = null;
        Map<List<Object>, List<Integer>> boxedIndex = null;
        if (fastPath) {
            FieldVector buildKey = buildKeyVectors.get(0);
            longIndex = new LinkedHashMap<>();
            for (int row = 0; row < buildRoot.getRowCount(); row++) {
                if (buildKey.isNull(row)) {
                    continue; // a null key component never matches -- standard SQL join semantics
                }
                longIndex.computeIfAbsent(encodeLong(buildKey, row), k -> new ArrayList<>()).add(row);
            }
        } else {
            boxedIndex = new LinkedHashMap<>();
            for (int row = 0; row < buildRoot.getRowCount(); row++) {
                List<Object> key = keyTuple(buildKeyVectors, row);
                if (key == null) {
                    continue;
                }
                boxedIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }

        boolean semiOrAnti = type == JoinType.SEMI || type == JoinType.ANTI;

        List<Field> outputFields = new ArrayList<>();
        for (Field field : left.getSchema().getFields()) {
            outputFields.add(leftAlias == null ? field : renamed(field, leftAlias));
        }
        if (!semiOrAnti) {
            // SEMI/ANTI only ever assert "does a match exist" -- the build
            // (subquery) side's own columns never appear in the output,
            // unlike every other join type.
            for (Field field : right.getSchema().getFields()) {
                outputFields.add(rightAlias == null ? field : renamed(field, rightAlias));
            }
        }
        VectorSchemaRoot result = VectorSchemaRoot.create(new Schema(outputFields), allocator);
        result.allocateNew();

        List<FieldVector> leftAllVectors = left.getFieldVectors();
        List<FieldVector> rightAllVectors = right.getFieldVectors();
        int leftFieldCount = leftAllVectors.size();
        int rightFieldCount = rightAllVectors.size();

        // Only FULL needs this -- it's the only type with a second,
        // unmatched-build-side pass.
        BitSet matchedBuildRows = type == JoinType.FULL ? new BitSet(buildRoot.getRowCount()) : null;

        int outRow = 0;
        for (int p = 0; p < probeRoot.getRowCount(); p++) {
            List<Integer> matches;
            if (fastPath) {
                FieldVector probeKey = probeKeyVectors.get(0);
                matches = probeKey.isNull(p) ? null : longIndex.get(encodeLong(probeKey, p));
            } else {
                List<Object> key = keyTuple(probeKeyVectors, p);
                matches = key == null ? null : boxedIndex.get(key);
            }

            if (matches == null) {
                if (type == JoinType.INNER || type == JoinType.SEMI) {
                    continue; // dropped -- INNER's standard semantics; SEMI asserts "a match exists," and none did
                }
                if (type == JoinType.ANTI) {
                    // ANTI asserts "no match exists" -- this probe row qualifies, emit it once, own columns only.
                    outRow = emitProbeOnly(p, leftAllVectors, result, outRow);
                    continue;
                }
                // LEFT/FULL: buildIsRight is always true here (see above),
                // so the probe side is always "left" -- preserve it,
                // null-pad the right side.
                outRow = emitProbeOnlyNullPadded(p, leftAllVectors, rightFieldCount, result, outRow);
                continue;
            }

            if (type == JoinType.SEMI) {
                // A match exists -- emit this probe row once, regardless of
                // how many build-side rows matched (no fan-out: SEMI only
                // ever asserts existence, never multiplies rows).
                outRow = emitProbeOnly(p, leftAllVectors, result, outRow);
                continue;
            }
            if (type == JoinType.ANTI) {
                continue; // a match exists -- ANTI's condition fails, drop this probe row
            }

            for (int buildMatchRow : matches) {
                if (matchedBuildRows != null) {
                    matchedBuildRows.set(buildMatchRow);
                }
                int leftRow = buildIsRight ? p : buildMatchRow;
                int rightRow = buildIsRight ? buildMatchRow : p;
                outRow = emitMatchedPair(leftRow, rightRow, leftAllVectors, rightAllVectors, result, outRow);
            }
        }

        if (type == JoinType.FULL) {
            // Unmatched build-side (right) rows -- preserve them, null-pad
            // the left side. buildRoot == right whenever type == FULL (see
            // buildIsRight above), so buildMatchRows indexes right's rows.
            for (int r = 0; r < buildRoot.getRowCount(); r++) {
                if (!matchedBuildRows.get(r)) {
                    outRow = emitBuildOnlyNullPadded(r, leftFieldCount, rightAllVectors, result, outRow);
                }
            }
        }

        result.setRowCount(outRow);
        return result;
    }

    private static int emitMatchedPair(int leftRow, int rightRow, List<FieldVector> leftAllVectors,
                                        List<FieldVector> rightAllVectors, VectorSchemaRoot result, int outRow) {
        int col = 0;
        for (FieldVector v : leftAllVectors) {
            copyValue(v, leftRow, result.getVector(col++), outRow);
        }
        for (FieldVector v : rightAllVectors) {
            copyValue(v, rightRow, result.getVector(col++), outRow);
        }
        return outRow + 1;
    }

    /** SEMI/ANTI: the probe row's own columns only -- no right-side columns exist in this output schema at all. */
    private static int emitProbeOnly(int leftRow, List<FieldVector> leftAllVectors, VectorSchemaRoot result, int outRow) {
        int col = 0;
        for (FieldVector v : leftAllVectors) {
            copyValue(v, leftRow, result.getVector(col++), outRow);
        }
        return outRow + 1;
    }

    /** A probe-side (always left, for LEFT/FULL) row with no match -- its own columns copied, every right column null. */
    private static int emitProbeOnlyNullPadded(int leftRow, List<FieldVector> leftAllVectors, int rightFieldCount,
                                                 VectorSchemaRoot result, int outRow) {
        int col = 0;
        for (FieldVector v : leftAllVectors) {
            copyValue(v, leftRow, result.getVector(col++), outRow);
        }
        for (int i = 0; i < rightFieldCount; i++) {
            result.getVector(col++).setNull(outRow);
        }
        return outRow + 1;
    }

    /** An unmatched build-side (always right, for FULL) row -- every left column null, its own columns copied. */
    private static int emitBuildOnlyNullPadded(int rightRow, int leftFieldCount, List<FieldVector> rightAllVectors,
                                                VectorSchemaRoot result, int outRow) {
        int col = 0;
        for (int i = 0; i < leftFieldCount; i++) {
            result.getVector(col++).setNull(outRow);
        }
        for (FieldVector v : rightAllVectors) {
            copyValue(v, rightRow, result.getVector(col++), outRow);
        }
        return outRow + 1;
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
     * column (see the {@code fastPath} branch in {@link #join}) --
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
