package com.labuk.labukdb.core.logical;

import java.util.List;

/**
 * A two-table {@code INNER} equi-join (SQL engine Phase 3, increment 1
 * — no {@code LEFT}/{@code RIGHT}/{@code FULL}, no non-equi conditions,
 * no 3+-way joins). {@code left}/{@code right} are each typically a
 * {@link TableScan} (with its own {@link Filter} already absorbed by
 * {@link PushFilterIntoScan} before this node is reached in the
 * optimized tree). {@code keys} are physical, unqualified column names
 * on each side — see {@code core.query.SqlQueryTranslator}'s own
 * javadoc for why the SQL-level table alias never reaches this node or
 * anything beneath it.
 */
public record Join(LogicalPlan left, LogicalPlan right, List<JoinKey> keys) implements LogicalPlan {
}
