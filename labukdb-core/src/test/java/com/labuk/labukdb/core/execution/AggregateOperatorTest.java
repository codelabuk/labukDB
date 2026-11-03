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

    /** Single BigIntVector group-by column -- exercises the primitive fast path, not the boxed List&lt;Object&gt; path. */
    @Test
    void groupBy_onSinglePrimitiveColumn_usesFastPathAndComputesCorrectSums() {
        Field region = new Field("region_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(region, amount)), allocator)) {
            batch.allocateNew();
            BigIntVector regionVector = (BigIntVector) batch.getVector("region_id");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            long[] regions = {1, 2, 1, 2, 1};
            long[] amounts = {10, 100, 20, 200, 30};
            for (int i = 0; i < regions.length; i++) {
                regionVector.setSafe(i, regions[i]);
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(regions.length);

            try (VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch), List.of("region_id"),
                    List.of(new AggregateSpec("amount", AggregateFunction.SUM, "total")),
                    allocator)) {
                assertEquals(2, result.getRowCount());
                Map<Long, Double> totalsByRegion = new java.util.HashMap<>();
                BigIntVector regionOut = (BigIntVector) result.getVector("region_id");
                Float8Vector totalOut = (Float8Vector) result.getVector("total");
                for (int i = 0; i < result.getRowCount(); i++) {
                    totalsByRegion.put(regionOut.get(i), totalOut.get(i));
                }
                assertEquals(60.0, totalsByRegion.get(1L)); // 10+20+30
                assertEquals(300.0, totalsByRegion.get(2L)); // 100+200
            }
        }
    }

    /** SQL groups every NULL key value into one shared group -- not excluded, and distinct from the fast path's non-null groups. */
    @Test
    void groupBy_onSinglePrimitiveColumn_withNullValues_formsOneSharedNullGroup() {
        Field region = new Field("region_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(region, amount)), allocator)) {
            batch.allocateNew();
            BigIntVector regionVector = (BigIntVector) batch.getVector("region_id");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            regionVector.setSafe(0, 1);
            amountVector.setSafe(0, 10);
            regionVector.setNull(1);
            amountVector.setSafe(1, 5);
            regionVector.setNull(2);
            amountVector.setSafe(2, 7);
            batch.setRowCount(3);

            try (VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch), List.of("region_id"),
                    List.of(new AggregateSpec("amount", AggregateFunction.SUM, "total"),
                            new AggregateSpec("amount", AggregateFunction.COUNT, "cnt")),
                    allocator)) {
                assertEquals(2, result.getRowCount(), "one group for region_id=1, one shared group for both nulls -- not 3, not 1");

                BigIntVector regionOut = (BigIntVector) result.getVector("region_id");
                Float8Vector totalOut = (Float8Vector) result.getVector("total");
                BigIntVector cntOut = (BigIntVector) result.getVector("cnt");

                boolean sawNullGroup = false;
                for (int i = 0; i < result.getRowCount(); i++) {
                    if (regionOut.isNull(i)) {
                        sawNullGroup = true;
                        assertEquals(12.0, totalOut.get(i), "both null-keyed rows (5+7) must be in the same group");
                        assertEquals(2L, cntOut.get(i));
                    } else {
                        assertEquals(1L, regionOut.get(i));
                        assertEquals(10.0, totalOut.get(i));
                        assertEquals(1L, cntOut.get(i));
                    }
                }
                assertTrue(sawNullGroup, "the null group must be present in the output, not silently dropped");
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
