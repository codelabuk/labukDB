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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code GROUP BY} + {@code SUM}/{@code COUNT}/{@code MIN}/{@code MAX}/
 * {@code AVG} over one or more already-decoded, already-filtered Arrow
 * batches — see this class's package javadoc-equivalent in
 * {@code docs/labukdb-v2-architecture.md} for why it's scoped this way:
 * assumes all input fits in memory on one process (same assumption
 * {@code WriteBuffer.flush()} already makes combining buffered batches),
 * and is deliberately not wired into {@code flight}'s per-file-endpoint
 * serving model yet — that needs a real protocol decision this class
 * doesn't make for you.
 *
 * {@code MIN}/{@code MAX} compare the row values this class already
 * extracts as boxed Java objects (String/Integer/Long/Double/Boolean —
 * all natively {@link Comparable}) rather than reaching for
 * {@code arrow-algorithm}'s vector-index comparators, which are built for
 * comparing rows *within* a still-attached vector, not already-detached
 * scalar values — simpler and equally correct for this case.
 */
public final class AggregateOperator {

    public VectorSchemaRoot aggregate(
            List<VectorSchemaRoot> inputBatches,
            List<String> groupByColumns,
            List<AggregateSpec> aggregates,
            BufferAllocator allocator) {

        if (inputBatches.isEmpty()) {
            throw new IllegalArgumentException("At least one input batch is required");
        }

        Map<List<Object>, Map<AggregateSpec, Accumulator>> groups = new LinkedHashMap<>();

        for (VectorSchemaRoot batch : inputBatches) {
            List<FieldVector> groupVectors = groupByColumns.stream().map(batch::getVector).toList();
            List<FieldVector> aggVectors = aggregates.stream().map(spec -> batch.getVector(spec.column())).toList();

            for (int row = 0; row < batch.getRowCount(); row++) {
                List<Object> key = new ArrayList<>(groupVectors.size());
                for (FieldVector v : groupVectors) {
                    key.add(getValue(v, row));
                }

                Map<AggregateSpec, Accumulator> accumulators =
                        groups.computeIfAbsent(key, k -> newAccumulators(aggregates));

                for (int i = 0; i < aggregates.size(); i++) {
                    AggregateSpec spec = aggregates.get(i);
                    Object value = getValue(aggVectors.get(i), row);
                    accumulators.get(spec).accept(value);
                }
            }
        }

        return buildResult(groups, groupByColumns, aggregates, inputBatches.get(0).getSchema(), allocator);
    }

    private Map<AggregateSpec, Accumulator> newAccumulators(List<AggregateSpec> aggregates) {
        Map<AggregateSpec, Accumulator> map = new LinkedHashMap<>();
        for (AggregateSpec spec : aggregates) {
            map.put(spec, new Accumulator());
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private VectorSchemaRoot buildResult(
            Map<List<Object>, Map<AggregateSpec, Accumulator>> groups,
            List<String> groupByColumns,
            List<AggregateSpec> aggregates,
            Schema inputSchema,
            BufferAllocator allocator) {

        List<Field> outputFields = new ArrayList<>();
        for (String column : groupByColumns) {
            outputFields.add(inputSchema.findField(column));
        }
        for (AggregateSpec spec : aggregates) {
            outputFields.add(outputFieldFor(spec, inputSchema));
        }

        VectorSchemaRoot result = VectorSchemaRoot.create(new Schema(outputFields), allocator);
        result.allocateNew();

        int row = 0;
        for (Map.Entry<List<Object>, Map<AggregateSpec, Accumulator>> entry : groups.entrySet()) {
            int col = 0;
            for (Object groupValue : entry.getKey()) {
                setValue(result.getVector(col++), row, groupValue);
            }
            for (AggregateSpec spec : aggregates) {
                Accumulator acc = entry.getValue().get(spec);
                setValue(result.getVector(col++), row, acc.result(spec.function()));
            }
            row++;
        }
        result.setRowCount(row);
        return result;
    }

    private Field outputFieldFor(AggregateSpec spec, Schema inputSchema) {
        return switch (spec.function()) {
            case COUNT -> new Field(spec.outputName(), FieldType.nullable(new ArrowType.Int(64, true)), null);
            case SUM, AVG -> new Field(spec.outputName(),
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            case MIN, MAX -> {
                Field sourceField = inputSchema.findField(spec.column());
                yield new Field(spec.outputName(), sourceField.getFieldType(), null);
            }
        };
    }

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

    @SuppressWarnings("unchecked")
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

    /** Running state for one group's one {@link AggregateSpec}. */
    private static final class Accumulator {
        private double sum = 0;
        private long count = 0;
        private Object min;
        private Object max;

        @SuppressWarnings("unchecked")
        void accept(Object value) {
            if (value == null) {
                return; // SQL-style: aggregates skip nulls rather than propagating them
            }
            count++;
            if (value instanceof Number n) {
                sum += n.doubleValue();
            }
            Comparable<Object> comparable = (Comparable<Object>) value;
            if (min == null || comparable.compareTo(min) < 0) {
                min = value;
            }
            if (max == null || comparable.compareTo(max) > 0) {
                max = value;
            }
        }

        Object result(AggregateFunction function) {
            return switch (function) {
                case SUM -> sum;
                case COUNT -> count;
                case AVG -> count == 0 ? null : sum / count;
                case MIN -> min;
                case MAX -> max;
            };
        }
    }
}
