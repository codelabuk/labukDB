package com.labuk.deltaflight.planner;

import java.util.HashSet;
import java.util.Set;

/**
 * Collects every column name referenced anywhere in a predicate tree.
 * Used by {@link com.labuk.deltaflight.decode.ParquetToArrowReader} to
 * ensure predicate columns are always included in the physical Parquet
 * read set, even when excluded from the requested output projection.
 *
 * The bug this fixes, concretely: a query requesting
 * {@code columns: ["company", "title"]} with a predicate on
 * {@code is_active} would previously build a Parquet read schema with
 * only {@code company} and {@code title}, then try to evaluate
 * {@code is_active = false} in {@link
 * com.labuk.deltaflight.decode.GroupPredicateEvaluator} against a row
 * that never had {@code is_active} decoded into it — producing
 * {@code InvalidRecordException: is_active not found in message
 * spark_schema}.
 */
public final class PredicateColumns {

    private PredicateColumns() {
    }

    public static Set<String> collect(Predicate predicate) {
        Set<String> columns = new HashSet<>();
        collectInto(predicate, columns);
        return columns;
    }

    private static void collectInto(Predicate predicate, Set<String> columns) {
        if (predicate == null) {
            return;
        }
        if (predicate instanceof Predicate.Comparison cmp) {
            columns.add(cmp.column);
        } else if (predicate instanceof Predicate.In in) {
            columns.add(in.column);
        } else if (predicate instanceof Predicate.Like like) {
            columns.add(like.column);
        } else if (predicate instanceof Predicate.And and) {
            and.children.forEach(child -> collectInto(child, columns));
        } else if (predicate instanceof Predicate.Or or) {
            or.children.forEach(child -> collectInto(child, columns));
        }
    }
}
