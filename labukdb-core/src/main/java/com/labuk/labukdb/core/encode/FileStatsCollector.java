package com.labuk.labukdb.core.encode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Computes the {@code numRecords}/{@code minValues}/{@code maxValues}/
 * {@code nullCount} JSON blob {@link com.labuk.labukdb.core.log.action.AddFile#getStats()}
 * carries — real Delta's own stats shape (confirmed against a genuine
 * Spark-produced commit this session: {@code {"numRecords":2,"minValues":
 * {"id":1,...},"maxValues":{"id":3,...},"nullCount":{"id":0,...}}}), not
 * invented here. {@link com.labuk.labukdb.core.planner.FileStatsPruner}
 * only ever reads {@code minValues}/{@code maxValues} today, but all four
 * fields are written to match the real protocol shape rather than a
 * narrower one this project happens to currently consume.
 *
 * <b>Column type coverage, stated plainly:</b> min/max is computed for
 * exactly the primitive types {@code HashJoinOperator}/{@code
 * AggregateOperator} already handle elsewhere in this codebase —
 * {@code Int}/{@code BigInt}/{@code Float4}/{@code Float8}/{@code Bit}/
 * {@code VarChar}/{@code TimeStampMicro}. {@code DecimalVector} and nested
 * ({@code LIST}/{@code MAP}/{@code STRUCT}) columns get no min/max — a
 * named gap, not a silent one: {@link
 * com.labuk.labukdb.core.planner.FileStatsPruner} already treats a column
 * with no min/max as "can't prune, keep the file," the same fail-open
 * behavior a file with no stats at all gets, so omitting these columns'
 * min/max is safe, not a correctness issue. {@code nullCount} is computed
 * for every column regardless of type, via {@link FieldVector#getNullCount()} —
 * that needs no type-specific comparison logic at all.
 *
 * <b>Timestamp stats are ISO-8601 strings</b>, matching real Delta/Parquet's
 * own convention (see {@code PredicateEvaluation#coerce}'s own javadoc for
 * why) — not raw epoch-micros longs, which would silently fail to compare
 * against a predicate's {@code Instant} literal.
 */
public final class FileStatsCollector {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FileStatsCollector() {
    }

    public static String collect(VectorSchemaRoot batch) {
        int rowCount = batch.getRowCount();
        Map<String, Object> minValues = new LinkedHashMap<>();
        Map<String, Object> maxValues = new LinkedHashMap<>();
        Map<String, Long> nullCount = new LinkedHashMap<>();

        for (FieldVector vector : batch.getFieldVectors()) {
            String name = vector.getField().getName();
            nullCount.put(name, (long) vector.getNullCount());

            MinMax minMax = minMax(vector, rowCount);
            if (minMax != null) {
                if (minMax.min() != null) {
                    minValues.put(name, minMax.min());
                }
                if (minMax.max() != null) {
                    maxValues.put(name, minMax.max());
                }
            }
        }

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("numRecords", rowCount);
        stats.put("minValues", minValues);
        stats.put("maxValues", maxValues);
        stats.put("nullCount", nullCount);
        try {
            return MAPPER.writeValueAsString(stats);
        } catch (JsonProcessingException e) {
            // Every value above is a String/Long/Double/Boolean -- Jackson
            // cannot fail serializing those; a real failure here would be a
            // programming error, not a data condition to handle gracefully.
            throw new IllegalStateException("Unexpected failure serializing file stats", e);
        }
    }

    private record MinMax(Object min, Object max) {
    }

    /** {@code null} means "no min/max collected for this column" -- see class javadoc for which types that applies to. */
    private static MinMax minMax(FieldVector vector, int rowCount) {
        if (vector instanceof IntVector v) {
            return minMaxLong(rowCount, i -> v.isNull(i) ? null : (long) v.get(i));
        }
        if (vector instanceof BigIntVector v) {
            return minMaxLong(rowCount, i -> v.isNull(i) ? null : v.get(i));
        }
        if (vector instanceof Float4Vector v) {
            return minMaxDouble(rowCount, i -> v.isNull(i) ? null : (double) v.get(i));
        }
        if (vector instanceof Float8Vector v) {
            return minMaxDouble(rowCount, i -> v.isNull(i) ? null : v.get(i));
        }
        if (vector instanceof BitVector v) {
            return minMaxBoolean(rowCount, i -> v.isNull(i) ? null : v.get(i) != 0);
        }
        if (vector instanceof VarCharVector v) {
            return minMaxString(rowCount, i -> v.isNull(i) ? null : new String(v.get(i), StandardCharsets.UTF_8));
        }
        if (vector instanceof TimeStampMicroVector v) {
            return minMaxInstant(rowCount, i -> v.isNull(i) ? null : v.get(i));
        }
        return null;
    }

    private static MinMax minMaxLong(int rowCount, java.util.function.IntFunction<Long> extractor) {
        Long min = null, max = null;
        for (int i = 0; i < rowCount; i++) {
            Long v = extractor.apply(i);
            if (v == null) {
                continue;
            }
            if (min == null || v < min) {
                min = v;
            }
            if (max == null || v > max) {
                max = v;
            }
        }
        return new MinMax(min, max);
    }

    private static MinMax minMaxDouble(int rowCount, java.util.function.IntFunction<Double> extractor) {
        Double min = null, max = null;
        for (int i = 0; i < rowCount; i++) {
            Double v = extractor.apply(i);
            if (v == null) {
                continue;
            }
            if (min == null || v < min) {
                min = v;
            }
            if (max == null || v > max) {
                max = v;
            }
        }
        return new MinMax(min, max);
    }

    private static MinMax minMaxBoolean(int rowCount, java.util.function.IntFunction<Boolean> extractor) {
        Boolean min = null, max = null;
        for (int i = 0; i < rowCount; i++) {
            Boolean v = extractor.apply(i);
            if (v == null) {
                continue;
            }
            if (min == null || Boolean.compare(v, min) < 0) {
                min = v;
            }
            if (max == null || Boolean.compare(v, max) > 0) {
                max = v;
            }
        }
        return new MinMax(min, max);
    }

    private static MinMax minMaxString(int rowCount, java.util.function.IntFunction<String> extractor) {
        String min = null, max = null;
        for (int i = 0; i < rowCount; i++) {
            String v = extractor.apply(i);
            if (v == null) {
                continue;
            }
            if (min == null || v.compareTo(min) < 0) {
                min = v;
            }
            if (max == null || v.compareTo(max) > 0) {
                max = v;
            }
        }
        return new MinMax(min, max);
    }

    /** Tracks min/max as raw epoch-micros internally (cheap long comparison), converts to an ISO-8601 string only once, at the end. */
    private static MinMax minMaxInstant(int rowCount, java.util.function.IntFunction<Long> extractor) {
        Long minMicros = null, maxMicros = null;
        for (int i = 0; i < rowCount; i++) {
            Long v = extractor.apply(i);
            if (v == null) {
                continue;
            }
            if (minMicros == null || v < minMicros) {
                minMicros = v;
            }
            if (maxMicros == null || v > maxMicros) {
                maxMicros = v;
            }
        }
        if (minMicros == null) {
            return new MinMax(null, null);
        }
        return new MinMax(microsToIso(minMicros), microsToIso(maxMicros));
    }

    private static String microsToIso(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1000L;
        return Instant.ofEpochSecond(seconds, nanos).toString();
    }
}
