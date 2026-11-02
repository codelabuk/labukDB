package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.query.SqlQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps an already-translated {@link SqlQuery} (parsed and validated by
 * {@code SqlQueryTranslator} — untouched by this class; that translator's
 * JSqlParser-AST walking, including its documented IN/OR grammar-quirk
 * detection, is already correct and tested) into the proper {@link
 * LogicalPlan} tree shape, omitting each layer the query doesn't use.
 */
public final class LogicalPlanBuilder {

    private LogicalPlanBuilder() {
    }

    public static LogicalPlan build(SqlQuery query) {
        if (query.join() != null) {
            return buildJoin(query);
        }

        LogicalPlan plan = new TableScan(query.table(), null, null);
        if (query.predicate() != null) {
            plan = new Filter(plan, query.predicate());
        }
        if (!query.groupByColumns().isEmpty() || !query.aggregates().isEmpty()) {
            plan = new Aggregate(plan, query.groupByColumns(), query.aggregates());
        }
        if (!query.orderBy().isEmpty()) {
            plan = new Sort(plan, query.orderBy());
        }
        if (query.limit() >= 0) {
            plan = new Limit(plan, query.limit());
        }
        return new Project(plan, query.columns());
    }

    /**
     * SQL engine Phase 3, increment 1. Each side's own {@code Filter} is
     * placed directly on its own {@code TableScan} here, exactly like
     * the single-table path above — {@link PushFilterIntoScan}'s
     * existing recursive absorption needs no new logic to handle this,
     * only a new case telling it to recurse into a {@link Join}'s two
     * children (see that class).
     */
    private static LogicalPlan buildJoin(SqlQuery query) {
        SqlQuery.JoinClause join = query.join();

        LogicalPlan left = new TableScan(query.table(), null, null);
        if (join.leftPredicate() != null) {
            left = new Filter(left, join.leftPredicate());
        }

        LogicalPlan right = new TableScan(join.rightTable(), null, null);
        if (join.rightPredicate() != null) {
            right = new Filter(right, join.rightPredicate());
        }

        List<JoinKey> keys = new ArrayList<>();
        for (SqlQuery.JoinKeyPair pair : join.keys()) {
            keys.add(new JoinKey(pair.leftColumn(), pair.rightColumn()));
        }

        return new Project(new Join(left, right, keys), query.columns());
    }
}
