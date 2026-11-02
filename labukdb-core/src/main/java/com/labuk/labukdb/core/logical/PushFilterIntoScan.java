package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.planner.Predicate;

/**
 * Finds {@code Filter(TableScan)} and merges the {@code WHERE} predicate
 * directly onto the scan, removing the {@link Filter} node — the IR-level
 * expression of what {@code QueryPlanner}'s partition/file-stats pruning
 * already does physically. {@link LogicalPlanBuilder} only ever places a
 * {@link Filter} directly on a {@link TableScan} (single-table queries,
 * Phase 1's whole scope), so that's the only shape this needs to handle —
 * written as a general recursive rewrite anyway so it keeps working once a
 * deeper tree (joins, Phase 3) exists. Deliberately no {@code switch}
 * pattern-matching over the sealed type here — this project targets Java
 * 17 (no preview features enabled), where that needs plain {@code
 * instanceof} chains instead.
 */
public final class PushFilterIntoScan implements OptimizerRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof Filter filter) {
            LogicalPlan rewrittenInput = apply(filter.input());
            if (rewrittenInput instanceof TableScan scan) {
                Predicate merged = scan.predicate() == null
                        ? filter.condition()
                        : Predicate.and(scan.predicate(), filter.condition());
                return new TableScan(scan.tableName(), merged, scan.requestedColumns());
            }
            return new Filter(rewrittenInput, filter.condition());
        }
        if (plan instanceof Aggregate aggregate) {
            return new Aggregate(apply(aggregate.input()), aggregate.groupByColumns(), aggregate.aggregates());
        }
        if (plan instanceof Sort sort) {
            return new Sort(apply(sort.input()), sort.sortKeys());
        }
        if (plan instanceof Limit limit) {
            return new Limit(apply(limit.input()), limit.limit());
        }
        if (plan instanceof Project project) {
            return new Project(apply(project.input()), project.columns());
        }
        return plan; // TableScan with nothing above it -- nothing to push
    }
}
