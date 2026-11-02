package com.labuk.labukdb.core.logical;

/**
 * Phase 1 of the SQL engine architecture rewrite: a real logical-plan tree,
 * replacing the flat {@code SqlQuery} record + hardcoded {@code
 * if (combineMode) {...} else {...}} branching {@code QueryCommand}/{@code
 * LabukDbFlightProducer} used to duplicate three ways (Delta streaming,
 * Delta combined, Iceberg-always-combined). Single-table only in this
 * phase — {@link TableScan} holds one table name, and {@code
 * SqlQueryTranslator} (untouched here) still rejects JOINs/subqueries at
 * parse time. See {@code docs/labukdb-v2-architecture.md}'s "Update —
 * SQL engine architecture: Phase 1" entry for the full Phase 1-4 arc.
 */
public sealed interface LogicalPlan permits TableScan, Filter, Aggregate, Sort, Limit, Project {
}
