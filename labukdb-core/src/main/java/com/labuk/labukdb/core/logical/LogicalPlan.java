package com.labuk.labukdb.core.logical;

/**
 * Phase 1 of the SQL engine architecture rewrite: a real logical-plan tree,
 * replacing the flat {@code SqlQuery} record + hardcoded {@code
 * if (combineMode) {...} else {...}} branching {@code QueryCommand}/{@code
 * LabukDbFlightProducer} used to duplicate three ways (Delta streaming,
 * Delta combined, Iceberg-always-combined).
 *
 * Single-table-per-side as of Phase 3 increment 1 — {@link Join} adds a
 * second {@link TableScan} (plus its own absorbed {@link Filter}), but
 * {@code SqlQueryTranslator} still rejects anything beyond one two-table
 * {@code INNER} equi-join (no 3+-way joins, no subqueries). See
 * {@code docs/labukdb-v2-architecture.md}'s "Update — SQL engine
 * architecture" entries for the full Phase 1-4 arc.
 */
public sealed interface LogicalPlan permits TableScan, Filter, Aggregate, Sort, Limit, Project, Join {
}
