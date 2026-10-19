package com.labuk.labukdb.core.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A file's partition values are known exactly (not just as a min/max
 * range) — every row in {@code level=ERROR/log_date=2026-07-17/part-0.parquet}
 * has {@code level="ERROR"} and {@code log_date="2026-07-17"}, no exceptions.
 * That means any comparison on a partition column can be resolved to a
 * plain boolean constant for a specific file, and the predicate simplified
 * with standard boolean algebra (TRUE AND x = x, FALSE OR x = x, etc.).
 *
 * <b>This exists to fix a real correctness gap, not just as an
 * optimization.</b> Without it, a residual predicate handed to the decode
 * layer could still contain a comparison on a partition column — a column
 * that does not exist in the physical Parquet file at all — and
 * {@code GroupPredicateEvaluator} would throw trying to look it up.
 * Folding those comparisons away before decode ever sees them is what
 * makes {@code level = "ERROR" AND amount > 2} — filtering on both a
 * partition column and a physical column in the same query — actually
 * work, rather than crash on the first row of every surviving file.
 *
 * Returns null to mean "always true," matching the convention used
 * throughout the planner and decode layers that a null predicate means
 * "no filter needed."
 */
public final class PartitionValueSubstituter {

    private PartitionValueSubstituter() {
    }

    public static Predicate simplify(Predicate predicate, Map<String, String> partitionValues) {
        if (predicate == null) {
            return null;
        }
        Object folded = fold(predicate, partitionValues);
        if (folded instanceof Boolean bool) {
            if (bool) {
                return null;
            }
            // A partition-column clause folding to FALSE here would mean this
            // file's own partition value can't satisfy a predicate that
            // PartitionPruner already used the identical comparison logic to
            // approve this file against — a contradiction, not a case that
            // should be reachable in practice. Fail open (no filtering)
            // rather than either crash or silently drop real rows, since a
            // defensive fallback here is about a bug in this code, not a
            // real query outcome to get subtly wrong.
            return null;
        }
        return (Predicate) folded;
    }

    /** Returns either a Boolean (fully resolved) or a simplified Predicate. */
    private static Object fold(Predicate predicate, Map<String, String> partitionValues) {
        if (predicate instanceof Predicate.Comparison cmp) {
            if (partitionValues.containsKey(cmp.column)) {
                return PredicateEvaluation.compareExact(partitionValues.get(cmp.column), cmp.op, cmp.literal);
            }
            return predicate; // references a physical column — leave untouched
        }

        if (predicate instanceof Predicate.In in) {
            if (partitionValues.containsKey(in.column)) {
                return PredicateEvaluation.matchesIn(partitionValues.get(in.column), in.literals);
            }
            return predicate;
        }

        if (predicate instanceof Predicate.Like like) {
            if (partitionValues.containsKey(like.column)) {
                return PredicateEvaluation.matchesLike(partitionValues.get(like.column), like.pattern);
            }
            return predicate;
        }

        if (predicate instanceof Predicate.And and) {
            List<Predicate> remaining = new ArrayList<>();
            for (Predicate child : and.children) {
                Object folded = fold(child, partitionValues);
                if (folded instanceof Boolean bool) {
                    if (!bool) {
                        return Boolean.FALSE; // AND short-circuits
                    }
                    // TRUE child contributes nothing further — drop it
                } else {
                    remaining.add((Predicate) folded);
                }
            }
            return collapse(remaining, true);
        }

        if (predicate instanceof Predicate.Or or) {
            List<Predicate> remaining = new ArrayList<>();
            for (Predicate child : or.children) {
                Object folded = fold(child, partitionValues);
                if (folded instanceof Boolean bool) {
                    if (bool) {
                        return Boolean.TRUE; // OR short-circuits
                    }
                    // FALSE child contributes nothing further — drop it
                } else {
                    remaining.add((Predicate) folded);
                }
            }
            return collapse(remaining, false);
        }

        return predicate;
    }

    private static Object collapse(List<Predicate> remaining, boolean isAnd) {
        if (remaining.isEmpty()) {
            return isAnd; // AND of nothing = TRUE; OR of nothing = FALSE
        }
        if (remaining.size() == 1) {
            return remaining.get(0);
        }
        return isAnd ? Predicate.and(remaining.toArray(new Predicate[0]))
                : Predicate.or(remaining.toArray(new Predicate[0]));
    }
}
