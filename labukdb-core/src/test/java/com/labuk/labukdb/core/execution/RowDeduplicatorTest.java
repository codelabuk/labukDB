package com.labuk.labukdb.core.execution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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

class RowDeduplicatorTest {

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String[] services, Long[] amounts) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();
        VarCharVector sv = (VarCharVector) root.getVector("service");
        BigIntVector av = (BigIntVector) root.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            if (services[i] == null) {
                sv.setNull(i);
            } else {
                sv.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            }
            if (amounts[i] == null) {
                av.setNull(i);
            } else {
                av.setSafe(i, amounts[i]);
            }
        }
        root.setRowCount(services.length);
        return root;
    }

    private List<String> servicesOf(VectorSchemaRoot result) {
        VarCharVector v = (VarCharVector) result.getVector("service");
        List<String> out = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
            out.add(v.isNull(i) ? null : new String(v.get(i), StandardCharsets.UTF_8));
        }
        return out;
    }

    private List<Long> amountsOf(VectorSchemaRoot result) {
        BigIntVector v = (BigIntVector) result.getVector("amount");
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
            out.add(v.isNull(i) ? null : v.get(i));
        }
        return out;
    }

    @Test
    void duplicateRows_onlyFirstOccurrenceSurvives_originalOrderPreserved() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator,
                     new String[] {"a", "b", "a", "c", "b"},
                     new Long[] {1L, 2L, 1L, 3L, 2L});
             VectorSchemaRoot result = new RowDeduplicator().deduplicate(input, allocator)) {
            assertEquals(3, result.getRowCount());
            assertEquals(List.of("a", "b", "c"), servicesOf(result));
            assertEquals(List.of(1L, 2L, 3L), amountsOf(result));
        }
    }

    @Test
    void noDuplicates_everyRowSurvivesUnchanged() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[] {"a", "b"}, new Long[] {1L, 2L});
             VectorSchemaRoot result = new RowDeduplicator().deduplicate(input, allocator)) {
            assertEquals(2, result.getRowCount());
            assertEquals(List.of("a", "b"), servicesOf(result));
        }
    }

    @Test
    void sameValuesDifferentColumn_notTreatedAsDuplicate() {
        // "a"/2 and "b"/1 share values but not as a whole row -- must both survive.
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[] {"a", "b"}, new Long[] {2L, 1L});
             VectorSchemaRoot result = new RowDeduplicator().deduplicate(input, allocator)) {
            assertEquals(2, result.getRowCount());
        }
    }

    @Test
    void nullCells_treatedAsEqualToEachOther_dedupedCorrectly() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator,
                     new String[] {"a", null, "a", null},
                     new Long[] {1L, null, 1L, null});
             VectorSchemaRoot result = new RowDeduplicator().deduplicate(input, allocator)) {
            assertEquals(2, result.getRowCount());
            assertEquals(java.util.Arrays.asList("a", null), servicesOf(result));
            assertEquals(java.util.Arrays.asList(1L, null), amountsOf(result));
        }
    }

    @Test
    void emptyInput_producesEmptyResult() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot input = buildBatch(allocator, new String[0], new Long[0]);
             VectorSchemaRoot result = new RowDeduplicator().deduplicate(input, allocator)) {
            assertEquals(0, result.getRowCount());
        }
    }
}
