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
        if (!query.joins().isEmpty()) {
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
     * Folds {@code query.joins()} into a left-deep chain of {@link Join}
     * nodes: the base table joins the first step's table, that combined
     * result joins the second step's table, and so on. Each step's own
     * {@code Filter} is placed directly on its own fresh {@code
     * TableScan} — {@link PushFilterIntoScan}'s existing recursive
     * absorption needs no new logic to handle this, since it already
     * recurses into both of a {@link Join}'s children generically.
     *
     * {@code RIGHT} reaches this method and the {@link Join} node it
     * builds completely unchanged — unlike an earlier design that
     * considered canonicalizing it into a swapped {@code LEFT} here,
     * {@code core.execution.HashJoinOperator} itself delegates {@code
     * RIGHT} to its {@code LEFT} implementation at call time instead
     * (see that class's javadoc), which keeps this tree's left/right
     * shape a simple, unambiguous invariant: left is always
     * "accumulated so far," right is always "this step's fresh table" —
     * regardless of join type, at every step.
     */
    private static LogicalPlan buildJoin(SqlQuery query) {
        LogicalPlan accumulated = new TableScan(query.table(), null, null);
        if (query.predicate() != null) {
            accumulated = new Filter(accumulated, query.predicate());
        }

        List<SqlQuery.JoinStep> steps = query.joins();
        for (int i = 0; i < steps.size(); i++) {
            SqlQuery.JoinStep step = steps.get(i);

            LogicalPlan newTable = new TableScan(step.rightTable(), null, null);
            if (step.rightPredicate() != null) {
                newTable = new Filter(newTable, step.rightPredicate());
            }

            List<JoinKey> keys = new ArrayList<>();
            for (SqlQuery.JoinKeyPair pair : step.keys()) {
                // The accumulated side's column is bare only for this
                // chain's very first step (the base table hasn't been
                // through any join yet, so it isn't alias-prefixed); every
                // later step's accumulated side is already a
                // HashJoinOperator output, whose columns are alias-prefixed.
                String leftColumn = (i == 0) ? pair.leftColumn() : pair.leftTableAlias() + "." + pair.leftColumn();
                keys.add(new JoinKey(leftColumn, pair.rightColumn()));
            }

            accumulated = new Join(accumulated, newTable, keys, step.type());
        }

        // SQL engine Phase 3, increment 2: wraps the join chain exactly the
        // way build() above wraps a single-table TableScan --
        // AggregateOperator/SortLimitOperator run on the combined output
        // unchanged.
        LogicalPlan plan = accumulated;
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
