package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.JoinType;

import java.util.List;

/**
 * An equi-join — {@code INNER}, {@code LEFT}, {@code RIGHT}, or {@code
 * FULL} — no non-equi conditions. {@code left} is either a {@link
 * TableScan} (the base table, only for the first join of a chain) or
 * another {@code Join} (every later step of a 3+-way left-deep chain —
 * see {@code core.logical.LogicalPlanBuilder#buildJoin}); {@code right}
 * is always this step's own new table, a {@link TableScan} (with its
 * own {@link Filter} already absorbed by {@link PushFilterIntoScan}
 * before this node is reached in the optimized tree). This left/right
 * asymmetry (left = accumulated-so-far, right = always a single fresh
 * table) is a real invariant {@code LogicalPlanExecutor}/{@code
 * HashJoinOperator} both rely on, not just a naming convention.
 *
 * {@code keys} are physical column names on each side — see {@code
 * core.query.SqlQueryTranslator}'s own javadoc for why the SQL-level
 * table alias never reaches this node or anything beneath it. For a
 * chain's first step, both sides' names are bare; for a later step,
 * {@code keys}' left-side names are already alias-qualified (they refer
 * to a prior step's {@code HashJoinOperator} output, not a fresh
 * decode) — see {@link JoinKey}'s own javadoc.
 */
public record Join(LogicalPlan left, LogicalPlan right, List<JoinKey> keys, JoinType type) implements LogicalPlan {
}
