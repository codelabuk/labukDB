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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SortLimitOperatorTest {

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

    private List<Long> amountsOf(VectorSchemaRoot result) {
        BigIntVector v = (BigIntVector) result.getVector("amount");
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
            out.add(v.get(i));
        }
        return out;
    }

    @Test
    void ascending_singleKey() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[] {"a", "b", "c"}, new long[] {30, 10, 20})) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input, List.of(new SortKey("amount", true)), -1, allocator)) {
                assertEquals(List.of(10L, 20L, 30L), amountsOf(result));
            }
        }
    }

    @Test
    void descending_singleKey() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[] {"a", "b", "c"}, new long[] {30, 10, 20})) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input, List.of(new SortKey("amount", false)), -1, allocator)) {
                assertEquals(List.of(30L, 20L, 10L), amountsOf(result));
            }
        }
    }

    @Test
    void multiColumn_tieBreaksOnSecondKey() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator,
                     new String[] {"b", "a", "a", "c"}, new long[] {5, 5, 1, 2})) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input,
                    List.of(new SortKey("amount", true), new SortKey("service", true)),
                    -1, allocator)) {
                VarCharVector serviceOut = (VarCharVector) result.getVector("service");
                List<String> order = new ArrayList<>();
                for (int i = 0; i < result.getRowCount(); i++) {
                    order.add(new String(serviceOut.get(i), StandardCharsets.UTF_8));
                }
                // amounts 1,5,5,2 sorted ascending -> 1(a), 2(c), 5(a), 5(b) -- the two
                // amount=5 rows tie-break alphabetically on service: a before b.
                assertEquals(List.of("a", "c", "a", "b"), order);
            }
        }
    }

    @Test
    void limit_keepsCorrectTopNAfterSort_notArbitraryN() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator,
                     new String[] {"a", "b", "c", "d", "e"}, new long[] {50, 10, 40, 20, 30})) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input, List.of(new SortKey("amount", false)), 3, allocator)) {
                assertEquals(3, result.getRowCount());
                assertEquals(List.of(50L, 40L, 30L), amountsOf(result), "top 3 by amount descending, not the first 3 rows");
            }
        }
    }

    @Test
    void limitZeroOrNegative_returnsEverythingSorted() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[] {"a", "b"}, new long[] {2, 1})) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input, List.of(new SortKey("amount", true)), 0, allocator)) {
                assertEquals(2, result.getRowCount());
                assertEquals(List.of(1L, 2L), amountsOf(result));
            }
        }
    }

    /** CHANGE_ALGORITHM_THRESHOLD in IndexSorter is 15 — this exercises the quicksort path, not just small-array insertion sort. */
    @Test
    void largerInput_exercisesQuicksortPath_stillCorrect() {
        int n = 50;
        String[] services = new String[n];
        long[] amounts = new long[n];
        for (int i = 0; i < n; i++) {
            services[i] = "svc-" + i;
            amounts[i] = (i * 37) % n; // scrambled, not already sorted
        }
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, services, amounts)) {
            try (VectorSchemaRoot result = new SortLimitOperator().sortAndLimit(
                    input, List.of(new SortKey("amount", true)), -1, allocator)) {
                List<Long> sorted = amountsOf(result);
                assertEquals(n, sorted.size());
                for (int i = 1; i < sorted.size(); i++) {
                    assertTrue(sorted.get(i - 1) <= sorted.get(i), "not sorted at index " + i);
                }
            }
        }
    }

    @Test
    void composedWithAggregateOperator_topGroupsByTotalDescending() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator,
                     new String[] {"auth", "gateway", "auth", "payments", "gateway", "auth"},
                     new long[] {10, 5, 20, 100, 5, 5})) {

            try (VectorSchemaRoot aggregated = new AggregateOperator().aggregate(
                    List.of(batch), List.of("service"),
                    List.of(new AggregateSpec("amount", AggregateFunction.SUM, "total")),
                    allocator);
                 VectorSchemaRoot top2 = new SortLimitOperator().sortAndLimit(
                         aggregated, List.of(new SortKey("total", false)), 2, allocator)) {

                assertEquals(2, top2.getRowCount());
                VarCharVector serviceOut = (VarCharVector) top2.getVector("service");
                Float8Vector totalOut = (Float8Vector) top2.getVector("total");

                assertEquals("payments", new String(serviceOut.get(0), StandardCharsets.UTF_8));
                assertEquals(100.0, totalOut.get(0));
                assertEquals("auth", new String(serviceOut.get(1), StandardCharsets.UTF_8));
                assertEquals(35.0, totalOut.get(1)); // 10+20+5
            }
        }
    }
}
