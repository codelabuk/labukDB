package com.labuk.labukdb.core.execution;

/**
 * {@code RIGHT} reaches {@code core.logical.Join#type()} completely
 * unchanged — no canonicalization happens in {@code
 * core.logical.LogicalPlanBuilder}. It's resolved only at the very end,
 * inside {@link HashJoinOperator}, which delegates it to its {@code LEFT}
 * implementation with every argument swapped at call time. That keeps the
 * logical-plan tree's left/right shape a simple, unambiguous invariant
 * (left is always "accumulated so far," right is always "this step's
 * fresh table") regardless of join type.
 *
 * {@code SEMI}/{@code ANTI} are how SQL engine Phase 4's {@code IN}/
 * {@code EXISTS}/{@code NOT EXISTS} subqueries execute —
 * {@code SqlQueryTranslator} decorrelates a subquery (simple equality
 * correlation only, same shape discipline a real {@code JOIN ... ON}
 * already enforces) into an ordinary {@code Join} step using one of
 * these types, so the existing N-way join-chain machinery runs it with
 * no new execution concept. {@code SEMI} emits a probe row at most once
 * if it has any match; {@code ANTI} emits it at most once if it has
 * none — neither ever fans out on a duplicate build-side key, and
 * neither side's output includes the build (subquery) side's columns.
 *
 * {@code NOT_IN} is {@code ANTI} plus standard SQL's {@code NOT IN}
 * NULL-poisoning rule — a dedicated type rather than a flag, because
 * {@code NOT EXISTS} (plain {@code ANTI}) and {@code NOT IN} genuinely
 * differ here: {@code x NOT IN (SELECT y FROM b)} is {@code NOT (x = y1
 * OR x = y2 OR ...)}, so if {@code x} matches no non-null {@code y} but
 * {@code b}'s result column contains even one {@code NULL}, standard
 * SQL's three-valued logic makes the whole comparison {@code UNKNOWN}
 * (excluded), not {@code TRUE} (included) — {@code NOT EXISTS} has no
 * such case, since a {@code NULL} correlation column just makes that
 * one candidate row's own condition {@code UNKNOWN} (not a "match"),
 * never poisons the outer existence check itself. See {@link
 * HashJoinOperator}'s own javadoc for exactly where this is checked.
 *
 * {@code CROSS} joins, non-equi conditions, and {@code NATURAL} joins
 * are rejected at parse time ({@code SqlQueryTranslator}) — there is no
 * value for them here, by design, not an oversight.
 */
public enum JoinType {
    INNER, LEFT, RIGHT, FULL, SEMI, ANTI, NOT_IN
}
