package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.JoinType;
import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PushFilterIntoScanTest {

    @Test
    void filterDirectlyOnScan_absorbedIntoScanPredicate() {
        Predicate condition = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        LogicalPlan plan = new Filter(new TableScan("logs", null, null), condition);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        TableScan scan = assertInstanceOf(TableScan.class, result, "the Filter node must be removed");
        assertSame(condition, scan.predicate());
    }

    @Test
    void noFilter_leavesPlanUnchanged() {
        LogicalPlan plan = new TableScan("logs", null, null);
        assertSame(plan, new PushFilterIntoScan().apply(plan));
    }

    @Test
    void filterUnderAggregateSortLimit_stillAbsorbedIntoScan() {
        Predicate condition = Predicate.compare("amount", ComparisonOp.GT, 2L);
        LogicalPlan plan = new Limit(
                new Sort(
                        new Aggregate(
                                new Filter(new TableScan("logs", null, null), condition),
                                List.of("level"), List.of()),
                        List.of()),
                3);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Limit limit = assertInstanceOf(Limit.class, result);
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        Aggregate aggregate = assertInstanceOf(Aggregate.class, sort.input());
        TableScan scan = assertInstanceOf(TableScan.class, aggregate.input());
        assertSame(condition, scan.predicate());
    }

    /** SQL engine Phase 3, increment 1: each side's own Filter absorbs into its own TableScan independently. */
    @Test
    void joinWithPerSideFilters_absorbsEachIndependently() {
        Predicate leftCondition = Predicate.compare("amount", ComparisonOp.GT, 10L);
        Predicate rightCondition = Predicate.compare("active", ComparisonOp.EQ, "Y");
        LogicalPlan plan = new Join(
                new Filter(new TableScan("orders", null, null), leftCondition),
                new Filter(new TableScan("customers", null, null), rightCondition),
                List.of(new JoinKey("customer_id", "id")), JoinType.INNER);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Join join = assertInstanceOf(Join.class, result);
        TableScan leftScan = assertInstanceOf(TableScan.class, join.left(), "left Filter must be absorbed");
        assertSame(leftCondition, leftScan.predicate());
        TableScan rightScan = assertInstanceOf(TableScan.class, join.right(), "right Filter must be absorbed");
        assertSame(rightCondition, rightScan.predicate());
    }

    @Test
    void joinWithOnlyOneSideFiltered_otherSideStaysBare() {
        Predicate leftCondition = Predicate.compare("amount", ComparisonOp.GT, 10L);
        LogicalPlan plan = new Join(
                new Filter(new TableScan("orders", null, null), leftCondition),
                new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")), JoinType.INNER);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Join join = assertInstanceOf(Join.class, result);
        assertSame(leftCondition, ((TableScan) join.left()).predicate());
        assertNull(((TableScan) join.right()).predicate());
    }

    @Test
    void joinType_isPreservedThroughTheRewrite() {
        LogicalPlan plan = new Join(new TableScan("orders", null, null), new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")), JoinType.LEFT);

        Join result = assertInstanceOf(Join.class, new PushFilterIntoScan().apply(plan));
        assertEquals(JoinType.LEFT, result.type());
    }

    /** A 3+-way chain's later step: left() is itself a Join, not a TableScan -- the existing recursion must still find and absorb its nested Filter. */
    @Test
    void nestedJoinChain_absorbsFilterInsideTheInnerJoinToo() {
        Predicate innerCondition = Predicate.compare("amount", ComparisonOp.GT, 10L);
        Predicate outerCondition = Predicate.compare("active", ComparisonOp.EQ, "Y");
        Join innerJoin = new Join(
                new Filter(new TableScan("orders", null, null), innerCondition),
                new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")), JoinType.INNER);
        LogicalPlan plan = new Join(innerJoin,
                new Filter(new TableScan("regions", null, null), outerCondition),
                List.of(new JoinKey("customers.region_id", "id")), JoinType.INNER);

        Join outer = assertInstanceOf(Join.class, new PushFilterIntoScan().apply(plan));
        Join inner = assertInstanceOf(Join.class, outer.left(), "nested Join must survive, not be mistaken for a plain scan");
        assertSame(innerCondition, ((TableScan) inner.left()).predicate());
        assertSame(outerCondition, ((TableScan) outer.right()).predicate());
    }

    @Test
    void idempotent_onAlreadyOptimizedPlan() {
        Predicate condition = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        LogicalPlan already = new TableScan("logs", condition, null);

        assertSame(already, new PushFilterIntoScan().apply(already));
    }
}
