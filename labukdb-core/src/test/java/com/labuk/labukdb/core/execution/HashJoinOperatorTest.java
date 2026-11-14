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

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {

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

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(1, result.getRowCount(), "customer_id=999 (left) and id=2 (right) have no match -- inner join drops both");
            }
        }
    }

    @Test
    void duplicateBuildSideKey_producesTrueFanOut_notFirstMatchOnly() {
        // Two customers rows share id=1 (the BUILD side here) -- one orders row with customer_id=1 must join both.
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 1}, new String[] {"us-east", "us-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(2, result.getRowCount(), "a duplicate key on the build side must fan out, not match only the first");
            }
        }
    }

    @Test
    void nullKeyOnEitherSide_neverMatchesAnything() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {
            ((BigIntVector) customers.getVector("id")).setNull(0); // null key, same value otherwise

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
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

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    left, List.of("customer_id", "region"), "a", right, List.of("customer_id", "region"), "b", false, a)) {
                assertEquals(0, result.getRowCount(), "both key columns must match -- id matches but region doesn't");
            }
        }
    }

    @Test
    void leftJoin_unmatchedLeftRow_isPreservedWithRightColumnsNull() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1, 999}, new String[] {"auth-svc", "orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.LEFT,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(2, result.getRowCount(), "LEFT JOIN preserves every left row, matched or not");

                VarCharVector service = (VarCharVector) result.getVector("a.service");
                assertEquals("auth-svc", new String(service.get(0), StandardCharsets.UTF_8));
                assertEquals("orphan-svc", new String(service.get(1), StandardCharsets.UTF_8));

                BigIntVector rightId = (BigIntVector) result.getVector("b.id");
                assertFalse(rightId.isNull(0), "the matched row keeps its real right-side value");
                assertTrue(rightId.isNull(1), "the unmatched left row's right-side columns are null, not dropped");
            }
        }
    }

    @Test
    void rightJoin_delegatesToLeft_preservingTheRightSideInstead() {
        // customer_id=999 only exists on the left (orders); id=2 only exists on the right (customers).
        // RIGHT JOIN must preserve every *right* row, dropping the left-only orphan and null-padding the right-only one.
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1, 999}, new String[] {"auth-svc", "orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 2}, new String[] {"us-east", "eu-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.RIGHT,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(2, result.getRowCount(), "RIGHT JOIN preserves every right row -- id=1 (matched) and id=2 (unmatched)");

                VarCharVector region = (VarCharVector) result.getVector("b.region");
                assertEquals("us-east", new String(region.get(0), StandardCharsets.UTF_8));
                assertEquals("eu-west", new String(region.get(1), StandardCharsets.UTF_8));

                BigIntVector leftId = (BigIntVector) result.getVector("a.customer_id");
                assertFalse(leftId.isNull(0), "the matched row keeps its real left-side value");
                assertTrue(leftId.isNull(1), "the unmatched right row's left-side columns are null, not dropped");
            }
        }
    }

    @Test
    void fullJoin_preservesUnmatchedRowsOnBothSides() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1, 999}, new String[] {"auth-svc", "orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 2}, new String[] {"us-east", "eu-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.FULL,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(3, result.getRowCount(), "1 match + 1 unmatched left + 1 unmatched right");

                BigIntVector leftId = (BigIntVector) result.getVector("a.customer_id");
                BigIntVector rightId = (BigIntVector) result.getVector("b.id");
                // Row 0: the real match.
                assertFalse(leftId.isNull(0));
                assertFalse(rightId.isNull(0));
                // Row 1: unmatched left (customer_id=999), from the main probe pass.
                assertFalse(leftId.isNull(1));
                assertTrue(rightId.isNull(1));
                // Row 2: unmatched right (id=2), from the second pass.
                assertTrue(leftId.isNull(2));
                assertFalse(rightId.isNull(2));
            }
        }
    }

    @Test
    void innerJoin_buildFromLeft_givesTheSameResultAsBuildFromRight() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 1}, new String[] {"us-east", "us-west"})) {

            try (VectorSchemaRoot buildRight = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator);
                 VectorSchemaRoot buildLeft = new HashJoinOperator().join(JoinType.INNER,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", true, allocator)) {
                // buildFromLeft only picks which side gets hashed internally -- the
                // declared left-then-right output order/content must be identical.
                assertEquals(buildRight.getRowCount(), buildLeft.getRowCount());
                assertEquals(2, buildLeft.getRowCount(), "fan-out must still happen when the smaller (left) side is probed, not built");
            }
        }
    }

    @Test
    void nullLeftAlias_leavesLeftColumnNamesUnrenamed() {
        // Simulates a 3+-way chain's later step: "left" is already a prior
        // HashJoinOperator output (its columns already alias-qualified, e.g.
        // "x.customer_id"), which must pass through unrenamed -- leftAlias=null.
        Field alreadyQualifiedId = new Field("x.customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field alreadyQualifiedService = new Field("x.service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

        try (VectorSchemaRoot preRenamedOrders = VectorSchemaRoot.create(
                new Schema(List.of(alreadyQualifiedId, alreadyQualifiedService)), allocator);
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {
            preRenamedOrders.allocateNew();
            ((BigIntVector) preRenamedOrders.getVector("x.customer_id")).setSafe(0, 1L);
            ((VarCharVector) preRenamedOrders.getVector("x.service")).setSafe(0, "auth-svc".getBytes(StandardCharsets.UTF_8));
            preRenamedOrders.setRowCount(1);

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    preRenamedOrders, List.of("x.customer_id"), null, customers, List.of("id"), "b", false, allocator)) {
                assertNotNull(result.getVector("x.customer_id"), "left column name must pass through unchanged, not re-prefixed with \"null.\"");
                assertNotNull(result.getVector("b.id"));
            }
        }
    }

    // --- SQL engine Phase 4, increment 1: SEMI/ANTI (decorrelated IN/EXISTS subqueries) ---

    @Test
    void semiJoin_matchExists_emitsProbeRowOnce_noFanOut_leftColumnsOnly() {
        // Two customers rows share id=1 -- a real INNER join would fan out
        // to 2 rows; SEMI must emit the probe row exactly once regardless.
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 1}, new String[] {"us-east", "us-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.SEMI,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(1, result.getRowCount(), "SEMI never fans out, even on a duplicate build-side key");
                assertNotNull(result.getVector("a.customer_id"));
                assertNotNull(result.getVector("a.service"));
                assertNull(result.getVector("b.id"), "SEMI's output never includes the build (subquery) side's columns");
                assertNull(result.getVector("b.region"));
            }
        }
    }

    @Test
    void semiJoin_noMatch_dropsTheProbeRow() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {999}, new String[] {"orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.SEMI,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(0, result.getRowCount());
            }
        }
    }

    @Test
    void antiJoin_noMatch_emitsTheProbeRowOnce() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {999}, new String[] {"orphan-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.ANTI,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(1, result.getRowCount(), "ANTI asserts 'no match' -- the unmatched row qualifies");
                VarCharVector service = (VarCharVector) result.getVector("a.service");
                assertEquals("orphan-svc", new String(service.get(0), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void antiJoin_matchExists_dropsTheProbeRow() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1, 1}, new String[] {"us-east", "us-west"})) {

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.ANTI,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(0, result.getRowCount(), "a match exists -- ANTI's condition fails for this row");
            }
        }
    }

    @Test
    void semiJoin_nullProbeKey_neverMatches_evenAgainstANumericallyEqualBuildRow() {
        try (VectorSchemaRoot orders = ordersBatch(new long[] {1}, new String[] {"auth-svc"});
             VectorSchemaRoot customers = customersBatch(new long[] {1}, new String[] {"us-east"})) {
            ((BigIntVector) orders.getVector("customer_id")).setNull(0); // null probe key, same value otherwise

            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.SEMI,
                    orders, List.of("customer_id"), "a", customers, List.of("id"), "b", false, allocator)) {
                assertEquals(0, result.getRowCount(), "a null join key must never match, standard SQL semantics");
            }
        }
    }
}
