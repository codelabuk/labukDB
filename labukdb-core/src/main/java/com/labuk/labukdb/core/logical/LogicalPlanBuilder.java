package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.query.SqlQuery;

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
}
