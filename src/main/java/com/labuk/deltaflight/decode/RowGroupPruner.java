package com.labuk.deltaflight.decode;

import com.labuk.deltaflight.planner.Predicate;
import com.labuk.deltaflight.planner.PredicateEvaluation;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.List;

/**
 * Layer 3's own pruning step, one level below {@code FileStatsPruner}: once
 * a file is worth opening, its footer carries per-row-group min/max
 * statistics for every column — the same range-pruning question as before
 * ("could any row in [min, max] match?"), just against Parquet's own stats
 * instead of Delta's log-level ones, and at row-group granularity instead
 * of whole-file.
 *
 * Returns a mask aligned with the input block list (rather than a filtered
 * list) because {@link ParquetToArrowReader} needs to walk row groups in
 * file order, calling {@code skipNextRowGroup()} for the ones this mask
 * rules out — Parquet's reader API is sequential, not random-access, so the
 * caller needs to know "keep or skip" per position, not just the surviving
 * subset.
 */
final class RowGroupPruner {

    private RowGroupPruner() {
    }

    static boolean[] pruneMask(List<BlockMetaData> blocks, Predicate predicate) {
        boolean[] mask = new boolean[blocks.size()];
        for (int i = 0; i < blocks.size(); i++) {
            mask[i] = predicate == null || mightMatch(predicate, blocks.get(i));
        }
        return mask;
    }

    private static boolean mightMatch(Predicate predicate, BlockMetaData block) {
        if (predicate instanceof Predicate.And and) {
            for (Predicate child : and.children) {
                if (!mightMatch(child, block)) {
                    return false;
                }
            }
            return true;
        }
        if (predicate instanceof Predicate.Or or) {
            for (Predicate child : or.children) {
                if (mightMatch(child, block)) {
                    return true;
                }
            }
            return false;
        }
        if (predicate instanceof Predicate.Comparison cmp) {
            ColumnChunkMetaData column = findColumn(block, cmp.column);
            if (column == null) {
                return true;
            }
            Statistics<?> stats = column.getStatistics();
            if (stats == null || stats.isEmpty()) {
                return true;
            }
            Object min = toJavaValue(stats.genericGetMin(), column.getPrimitiveType().getPrimitiveTypeName());
            Object max = toJavaValue(stats.genericGetMax(), column.getPrimitiveType().getPrimitiveTypeName());
            return PredicateEvaluation.mightMatchRange(min, max, cmp.op, cmp.literal);
        }
        if (predicate instanceof Predicate.In in) {
            ColumnChunkMetaData column = findColumn(block, in.column);
            if (column == null) {
                return true;
            }
            Statistics<?> stats = column.getStatistics();
            if (stats == null || stats.isEmpty()) {
                return true;
            }
            Object min = toJavaValue(stats.genericGetMin(), column.getPrimitiveType().getPrimitiveTypeName());
            Object max = toJavaValue(stats.genericGetMax(), column.getPrimitiveType().getPrimitiveTypeName());
            return PredicateEvaluation.mightMatchRangeIn(min, max, in.literals);
        }
        if (predicate instanceof Predicate.Like) {
            return true; // same documented gap as FileStatsPruner — no prefix-range pruning for LIKE yet
        }
        return true;
    }

    private static ColumnChunkMetaData findColumn(BlockMetaData block, String columnName) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            if (column.getPath().toDotString().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    private static Object toJavaValue(Object statValue, PrimitiveTypeName typeName) {
        // Parquet's Binary stat type needs explicit handling to line up
        // with what a predicate would compare against: UTF-8 decode for a
        // real string column, but the INT96 timestamp codec for a
        // timestamp column — both arrive as the same raw Binary type from
        // genericGetMin/Max, so the column's actual Parquet type is what
        // tells them apart. Every other stat type (Integer, Long, Float,
        // Double, Boolean) is already a plain Comparable PredicateEvaluation
        // understands.
        if (statValue instanceof Binary binary) {
            if (typeName == PrimitiveTypeName.INT96) {
                return Int96TimestampCodec.toInstant(binary);
            }
            return binary.toStringUsingUTF8();
        }
        return statValue;
    }
}
