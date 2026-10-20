package com.labuk.labukdb.core.execution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregateOperatorTest {

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String[] services, long[] amounts) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();
        VarCharVector sv = (VarCharVector) root.getVector("service");
        BigIntVector av = (BigIntVector) root.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            sv.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            av.setSafe(i, amounts[i]);
        }
        root.setRowCount(services.length);
        return root;
    }

    private Map<String, Object> rowAsMap(VectorSchemaRoot result, int row) {
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        for (var vector : result.getFieldVectors()) {
            map.put(vector.getField().getName(), vector.getObject(row));
        }
        return map;
    }

    @Test
    void groupBy_withAllFiveFunctions_computesCorrectPerGroupValues() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch1 = buildBatch(allocator, new String[] {"auth", "auth", "gateway"}, new long[] {10, 20, 5});
             VectorSchemaRoot batch2 = buildBatch(allocator, new String[] {"auth", "gateway"}, new long[] {30, 15})) {

            VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch1, batch2),
                    List.of("service"),
                    List.of(
                            new AggregateSpec("amount", AggregateFunction.SUM, "total"),
                            new AggregateSpec("amount", AggregateFunction.COUNT, "cnt"),
                            new AggregateSpec("amount", AggregateFunction.AVG, "avg"),
                            new AggregateSpec("amount", AggregateFunction.MIN, "min"),
                            new AggregateSpec("amount", AggregateFunction.MAX, "max")),
                    allocator);
            try (result) {
                assertEquals(2, result.getRowCount(), "two distinct services -> two groups");

                Map<String, Object> auth = null, gateway = null;
                for (int i = 0; i < result.getRowCount(); i++) {
                    Map<String, Object> row = rowAsMap(result, i);
                    String service = row.get("service").toString();
                    if (service.equals("auth")) auth = row;
                    if (service.equals("gateway")) gateway = row;
                }

                assertNotNull(auth);
                assertEquals(60.0, (Double) auth.get("total")); // 10+20+30
                assertEquals(3L, auth.get("cnt"));
                assertEquals(20.0, (Double) auth.get("avg"));
                assertEquals(10L, auth.get("min"));
                assertEquals(30L, auth.get("max"));

                assertNotNull(gateway);
                assertEquals(20.0, (Double) gateway.get("total")); // 5+15
                assertEquals(2L, gateway.get("cnt"));
                assertEquals(10.0, (Double) gateway.get("avg"));
                assertEquals(5L, gateway.get("min"));
                assertEquals(15L, gateway.get("max"));
            }
        }
    }

    @Test
    void groupBy_withNoAggregates_returnsDistinctGroups() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"a", "b", "a", "a"}, new long[] {1, 2, 3, 4})) {

            try (VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch), List.of("service"), List.of(), allocator)) {
                assertEquals(2, result.getRowCount());
            }
        }
    }

    @Test
    void noGroupByColumns_producesOneGlobalGroup() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"a", "b", "c"}, new long[] {1, 2, 3})) {

            try (VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch), List.of(),
                    List.of(new AggregateSpec("amount", AggregateFunction.SUM, "total"),
                            new AggregateSpec("amount", AggregateFunction.COUNT, "cnt")),
                    allocator)) {
                assertEquals(1, result.getRowCount(), "no GROUP BY columns -> one global group");
                assertEquals(6.0, (Double) ((Float8Vector) result.getVector("total")).getObject(0));
                assertEquals(3L, ((BigIntVector) result.getVector("cnt")).get(0));
            }
        }
    }
}
