package com.labuk.labukdb.core.logical;

/**
 * A real logical-plan tree, replacing the flat {@code SqlQuery} record +
 * hardcoded {@code if (combineMode) {...} else {...}} branching {@code
 * QueryCommand}/{@code LabukDbFlightProducer} used to duplicate three
 * ways (Delta streaming, Delta combined, Iceberg-always-combined).
 *
 * {@link Join} adds a second {@link TableScan} (plus its own absorbed
 * {@link Filter}) and can sit underneath {@link Aggregate}/{@link Sort}/
 * {@link Limit} — a two-table {@code INNER} equi-join, optionally
 * combined with {@code GROUP BY}/{@code ORDER BY}/{@code LIMIT}, is
 * supported; {@code SqlQueryTranslator} still rejects anything beyond
 * that (no {@code LEFT}/{@code RIGHT}/{@code FULL}, no 3+-way joins, no
 * subqueries). See {@code docs/ARCHITECTURE.md}'s "SQL engine" section
 * for the current scope.
 */
public sealed interface LogicalPlan permits TableScan, Filter, Aggregate, Sort, Limit, Project, Join {
}
