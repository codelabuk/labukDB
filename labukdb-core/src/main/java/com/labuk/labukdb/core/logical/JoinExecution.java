package com.labuk.labukdb.core.logical;

import java.util.List;

/**
 * What {@link LogicalPlanExecutor#resolveJoin} produces: each side's own
 * independently-resolved {@link ResolvedScan} plus the physical
 * (unqualified) key column names on each side, ready to hand to {@code
 * CombinedQueryExecutor} (once per side, to fully materialize it) and
 * then {@code HashJoinOperator}.
 */
public record JoinExecution(ResolvedScan left, ResolvedScan right,
                             List<String> leftKeys, List<String> rightKeys) {
}
