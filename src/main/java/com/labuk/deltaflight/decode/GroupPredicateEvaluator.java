package com.labuk.deltaflight.decode;

import com.labuk.deltaflight.planner.Predicate;
import com.labuk.deltaflight.planner.PredicateEvaluation;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * The bottom of the pruning stack: whatever survived partition pruning,
 * file-stats pruning, and row-group pruning still needs an actual per-row
 * check, because none of the earlier layers can guarantee every row in a
 * surviving range matches — they only rule out ranges that definitely
 * don't. This is where a query's filter is applied exactly, row by row,
 * against real decoded values.
 *
 * NULL handling is intentionally SQL-style rather than reusing
 * PredicateEvaluation's "can't decide, fail open" behavior: fail-open exists
 * so pruning never wrongly drops real data, but at this layer we're looking
 * at the actual value, and a NULL genuinely doesn't satisfy any comparison —
 * so it's excluded here, not kept.
 *
 * Only flat (non-nested, non-repeated) primitive columns are supported —
 * matches the same scope limitation as {@link ArrowSchemaMapper}.
 */
final class GroupPredicateEvaluator {

    private GroupPredicateEvaluator() {
    }

    static boolean matches(Predicate predicate, Group row, MessageType schema) {
        if (predicate == null) {
            return true;
        }
        return evaluate(predicate, row, schema);
    }

    private static boolean evaluate(Predicate predicate, Group row, MessageType schema) {
        if (predicate instanceof Predicate.And and) {
            for (Predicate child : and.children) {
                if (!evaluate(child, row, schema)) {
                    return false;
                }
            }
            return true;
        }
        if (predicate instanceof Predicate.Or or) {
            for (Predicate child : or.children) {
                if (evaluate(child, row, schema)) {
                    return true;
                }
            }
            return false;
        }
        if (predicate instanceof Predicate.Comparison cmp) {
            Object actual = readValue(row, schema, cmp.column);
            if (actual == null) {
                return false; // SQL semantics: NULL comparisons are never true
            }
            return PredicateEvaluation.compareExact(actual, cmp.op, cmp.literal);
        }
        if (predicate instanceof Predicate.In in) {
            Object actual = readValue(row, schema, in.column);
            if (actual == null) {
                return false;
            }
            return PredicateEvaluation.matchesIn(actual, in.literals);
        }
        if (predicate instanceof Predicate.Like like) {
            Object actual = readValue(row, schema, like.column);
            if (actual == null) {
                return false;
            }
            return PredicateEvaluation.matchesLike(actual, like.pattern);
        }
        return true;
    }

    private static Object readValue(Group row, MessageType schema, String column) {
        if (row.getFieldRepetitionCount(column) == 0) {
            return null;
        }
        PrimitiveTypeName typeName = schema.getType(column).asPrimitiveType().getPrimitiveTypeName();
        switch (typeName) {
            case INT32:
                return (long) row.getInteger(column, 0); // widened so it lines up with Long-typed literals
            case INT64:
                return row.getLong(column, 0);
            case FLOAT:
                return (double) row.getFloat(column, 0);
            case DOUBLE:
                return row.getDouble(column, 0);
            case BOOLEAN:
                return row.getBoolean(column, 0);
            case BINARY:
                return row.getString(column, 0);
            case INT96:
                // Instant, not raw epoch micros — now that PredicateCodec
                // supports a real timestamp literal type, Instant is the one
                // consistent representation for timestamp comparisons across
                // every layer, rather than an epoch-micros-long convention
                // that only existed because the wire format used to lack a
                // timestamp literal.
                return Int96TimestampCodec.toInstant(row.getInt96(column, 0));
            default:
                throw new UnsupportedOperationException(
                        "Parquet type not supported yet: " + typeName + " (column '" + column + "')");
        }
    }
}
