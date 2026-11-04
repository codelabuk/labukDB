package com.labuk.labukdb.core.logical;

import java.util.List;

/**
 * What {@link LogicalPlanExecutor#resolveJoin} produces: each side's own
 * independently-resolved {@link ResolvedScan} plus the physical
 * (unqualified) key column names on each side, ready to hand to {@code
 * CombinedQueryExecutor} (once per side, to fully materialize it) and
 * then {@code HashJoinOperator}.
 *
 * {@code aggregate}/{@code sort}/{@code limit} (SQL engine Phase 3,
 * increment 2) mirror {@link ResolvedScan}'s own fields exactly, but
 * apply to the *join's combined output*, not either side individually —
 * {@code null} unless the SQL had a {@code GROUP BY}/aggregate/
 * {@code ORDER BY}/{@code LIMIT} combined with the {@code JOIN}.
 */
public record JoinExecution(ResolvedScan left, ResolvedScan right,
                             List<String> leftKeys, List<String> rightKeys,
                             Aggregate aggregate, Sort sort, Limit limit) {
}
