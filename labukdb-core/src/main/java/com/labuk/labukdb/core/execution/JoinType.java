package com.labuk.labukdb.core.execution;

/**
 * {@code RIGHT} is a parser-facing value only — {@code
 * core.logical.LogicalPlanBuilder} canonicalizes every {@code RIGHT} step
 * into an equivalent {@code LEFT} (swap sides, swap join keys) before
 * constructing a {@code core.logical.Join} node, so {@code
 * core.logical.Join#type()} and {@link HashJoinOperator} never see
 * {@code RIGHT} — only {@code INNER}, {@code LEFT}, and {@code FULL} are
 * real runtime join algorithms.
 *
 * {@code CROSS} joins, non-equi conditions, and {@code NATURAL}/{@code
 * SEMI} joins are rejected at parse time ({@code SqlQueryTranslator}) —
 * there is no value for them here, by design, not an oversight.
 */
public enum JoinType {
    INNER, LEFT, RIGHT, FULL
}
