package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Computes the full needed-column set for the whole plan and pushes it
 * onto the {@link TableScan}, then eliminates the top-level {@link
 * Project} entirely — always a no-op for a single-table query, since
 * there is no remaining physical work once the reader only decodes the
 * needed columns (confirmed: {@code QueryCommand}'s combine path already
 * prints whatever schema the combine executor naturally produces, never a
 * separately-tracked "trim to SELECT list" step).
 *
 * Exactly reproduces the union {@code QueryCommand.runSql} computed
 * inline before this rule existed: when aggregating, group-by +
 * aggregate-input columns (the original {@code SELECT} list is irrelevant
 * once {@code GROUP BY} is present — {@code SqlQueryTranslator} already
 * requires every {@code SELECT} item to be either a group-by column or an
 * aliased aggregate); otherwise, the {@code SELECT} list + {@code ORDER BY}
 * columns. {@code WHERE}-only columns need no explicit handling here —
 * {@code ParquetToArrowReader} already unions predicate columns in at
 * decode time (see {@code PredicateColumns.collect}), exactly as before
 * this rule existed.
 *
 * <b>Join exception (SQL engine Phase 3, increment 1):</b> the "no
 * remaining physical work" justification above only holds because a
 * single-table scan's {@code requestedColumns} ends up being exactly
 * the {@code SELECT} list. For a {@link Join}, column pruning is
 * deliberately NOT pushed across the join boundary in this increment
 * (each side's {@code TableScan} keeps {@code requestedColumns == null}
 * — "decode everything," a stated simplification, not an oversight) —
 * so the combined post-join output has more columns than the final
 * {@code SELECT} wants, and the outer {@link Project} is the only place
 * that final selection survives. It is therefore kept, not eliminated,
 * whenever the rewritten input is a {@link Join}.
 */
public final class PushProjectIntoScan implements OptimizerRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (!(plan instanceof Project project)) {
            return plan; // LogicalPlanBuilder always wraps the whole plan in exactly one top-level Project
        }

        Aggregate aggregate = findAggregate(project.input());
        Set<String> needed = new LinkedHashSet<>();
        if (aggregate != null) {
            needed.addAll(aggregate.groupByColumns());
            for (AggregateSpec spec : aggregate.aggregates()) {
                needed.add(spec.column());
            }
        } else {
            needed.addAll(project.columns());
            Sort sort = findSort(project.input());
            if (sort != null) {
                for (SortKey key : sort.sortKeys()) {
                    needed.add(key.column());
                }
            }
        }

        LogicalPlan rewrittenInput = pushInto(project.input(), List.copyOf(needed));
        if (aggregate == null && containsJoin(rewrittenInput)) {
            return new Project(rewrittenInput, project.columns());
        }
        return rewrittenInput;
    }

    /**
     * SQL engine Phase 3, increment 2: generalizes increment 1's bare-{@code
     * Join} check, which missed {@code Project(Limit(Sort(Join(...))))} --
     * a join + {@code ORDER BY}/{@code LIMIT} (no {@code GROUP BY}) query,
     * where {@code Sort}/{@code Limit} sit between {@code Project} and
     * {@code Join}. Without an {@code Aggregate} present, the join's full
     * combined columns are still wider than the real {@code SELECT} list
     * (column pruning isn't pushed across the join boundary), so {@code
     * Project} must survive regardless of how deep the {@code Join} sits.
     */
    private boolean containsJoin(LogicalPlan plan) {
        if (plan instanceof Join) {
            return true;
        }
        if (plan instanceof Filter filter) {
            return containsJoin(filter.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return containsJoin(aggregate.input());
        }
        if (plan instanceof Sort sort) {
            return containsJoin(sort.input());
        }
        if (plan instanceof Limit limit) {
            return containsJoin(limit.input());
        }
        return false;
    }

    private LogicalPlan pushInto(LogicalPlan plan, List<String> columns) {
        if (plan instanceof TableScan scan) {
            return new TableScan(scan.tableName(), scan.predicate(), columns);
        }
        if (plan instanceof Filter filter) {
            return new Filter(pushInto(filter.input(), columns), filter.condition());
        }
        if (plan instanceof Aggregate aggregate) {
            return new Aggregate(pushInto(aggregate.input(), columns), aggregate.groupByColumns(), aggregate.aggregates());
        }
        if (plan instanceof Sort sort) {
            return new Sort(pushInto(sort.input(), columns), sort.sortKeys());
        }
        if (plan instanceof Limit limit) {
            return new Limit(pushInto(limit.input(), columns), limit.limit());
        }
        // Join: deliberately NOT pushed into (see class javadoc) -- returned
        // unchanged, so both sides keep requestedColumns == null ("decode everything").
        return plan;
    }

    private Aggregate findAggregate(LogicalPlan plan) {
        if (plan instanceof Aggregate aggregate) {
            return aggregate;
        }
        if (plan instanceof Filter filter) {
            return findAggregate(filter.input());
        }
        if (plan instanceof Sort sort) {
            return findAggregate(sort.input());
        }
        if (plan instanceof Limit limit) {
            return findAggregate(limit.input());
        }
        return null;
    }

    private Sort findSort(LogicalPlan plan) {
        if (plan instanceof Sort sort) {
            return sort;
        }
        if (plan instanceof Filter filter) {
            return findSort(filter.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return findSort(aggregate.input());
        }
        if (plan instanceof Limit limit) {
            return findSort(limit.input());
        }
        return null;
    }
}
