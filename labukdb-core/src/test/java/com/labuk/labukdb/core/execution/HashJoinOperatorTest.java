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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Real inner-join correctness: matching/non-matching rows, true fan-out on a duplicate build-side key, alias-prefixed output names. */
class HashJoinOperatorTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private VectorSchemaRoot ordersBatch(long[] customerIds, String[] services) {
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(customerId, service)), allocator);
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("customer_id");
        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        for (int i = 0; i < customerIds.length; i++) {
            idVector.setSafe(i, customerIds[i]);
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(customerIds.length);
        return root;
    }

    private VectorSchemaRoot customersBatch(long[] ids, String[] regions) {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(id, region)), allocator);
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("id");
        VarCharVector regionVector = (VarCharVector) root.getVector("region");
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i]);
            regionVector.setSafe(i, regions[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(ids.length);
        return root;
    }

    @Test
    void matchingRows_combineCorrectly_withAliasPrefixedOutputNames() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1, 2}, new String[] {"auth-svc", "payments-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 2}, new String[] {"us-east", "eu-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().innerJoin(
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", allocator)) {

                assertEquals(2, result.getRowCount());
                assertNotNull(result.getVector("a.customer_id"));
                assertNotNull(result.getVector("a.service"));
                assertNotNull(result.getVector("b.id"));
                assertNotNull(result.getVector("b.region"));

                VarCharVector service = (VarCharVector) result.getVector("a.service");
                VarCharVector region = (VarCharVector) result.getVector("b.region");
                assertEquals("auth-svc", new String(service.get(0), StandardCharsets.UTF_8));
                assertEquals("us-east", new String(region.get(0), StandardCharsets.UTF_8));
                assertEquals("payments-svc", new String(service.get(1), StandardCharsets.UTF_8));
                assertEquals("eu-west", new String(region.get(1), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void nonMatchingRows_onEitherSide_areDropped() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1, 999}, new String[] {"auth-svc", "orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 2}, new String[] {"us-east", "eu-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().innerJoin(
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", allocator)) {
                assertEquals(1, result.getRowCount(), "customer_id=999 (left) and id=2 (right) have no match -- inner join drops both");
            }
        }
    }

    @Test
    void duplicateBuildSideKey_producesTrueFanOut_notFirstMatchOnly() {
        // Two customers rows share id=1 (the BUILD side here) -- one orders row with customer_id=1 must join both.
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 1}, new String[] {"us-east", "us-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().innerJoin(
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", allocator)) {
                assertEquals(2, result.getRowCount(), "a duplicate key on the build side must fan out, not match only the first");
            }
        }
    }

    @Test
    void nullKeyOnEitherSide_neverMatchesAnything() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {
            ((BigIntVector) customers.getVector("id")).setNull(0); // null key, same value otherwise

            try (VectorSchemaRoot result = new HashJoinOperator().innerJoin(
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", allocator)) {
                assertEquals(0, result.getRowCount(), "a null join key must never match, even a numerically-equal value");
            }
        }
    }

    @Test
    void multiKeyJoin_bothKeysMustMatch() {
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        try (BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot left = VectorSchemaRoot.create(new Schema(List.of(customerId, region)), a);
             VectorSchemaRoot right = VectorSchemaRoot.create(new Schema(List.of(customerId, region)), a)) {
            left.allocateNew();
            ((BigIntVector) left.getVector("customer_id")).setSafe(0, 1L);
            ((VarCharVector) left.getVector("region")).setSafe(0, "us".getBytes(StandardCharsets.UTF_8));
            left.setRowCount(1);

            right.allocateNew();
            ((BigIntVector) right.getVector("customer_id")).setSafe(0, 1L);
            ((VarCharVector) right.getVector("region")).setSafe(0, "eu".getBytes(StandardCharsets.UTF_8)); // same id, different region
            right.setRowCount(1);

            try (VectorSchemaRoot result = new HashJoinOperator().innerJoin(
                    left, List.of("customer_id", "region"), "a", right, List.of("customer_id", "region"), "b", a)) {
                assertEquals(0, result.getRowCount(), "both key columns must match -- id matches but region doesn't");
            }
        }
    }
}
