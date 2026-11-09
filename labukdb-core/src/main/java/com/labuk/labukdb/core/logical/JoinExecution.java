package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.JoinType;

import java.util.List;

/**
 * What {@link LogicalPlanExecutor#resolveJoin} produces: {@code base}'s
 * own independently-resolved {@link ResolvedScan} (the chain's first
 * table), followed by one {@link Step} per join in the left-deep chain,
 * each carrying that step's own new table's {@link ResolvedScan} plus
 * the physical key column names on each side — ready to hand to {@code
 * CombinedQueryExecutor} (once per table, to fully materialize it) and
 * then {@code HashJoinOperator}, sequentially, one step at a time.
 *
 * {@code aggregate}/{@code sort}/{@code limit} mirror {@link
 * ResolvedScan}'s own fields exactly, but apply to the whole chain's
 * final combined output, not any one table individually — {@code null}
 * unless the SQL had a {@code GROUP BY}/aggregate/{@code ORDER BY}/
 * {@code LIMIT} combined with the join.
 */
public record JoinExecution(ResolvedScan base, List<Step> steps, Aggregate aggregate, Sort sort, Limit limit) {

    /**
     * {@code leftKeys} names the accumulated side's key columns as they
     * already exist at this point in the chain — bare for step 0 (the
     * base table hasn't been joined yet), alias-qualified for every
     * later step (a prior {@code HashJoinOperator} output). {@code
     * rightKeys} are always bare ({@code table}, this step's own fresh
     * table, has never been joined before). See {@code
     * core.logical.LogicalPlanBuilder}'s own javadoc for why.
     */
    public record Step(JoinType type, ResolvedScan table, List<String> leftKeys, List<String> rightKeys) {
    }
}
