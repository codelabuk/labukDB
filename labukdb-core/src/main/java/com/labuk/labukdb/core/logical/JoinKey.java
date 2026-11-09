package com.labuk.labukdb.core.logical;

/**
 * One {@code ON a.x = b.y} equality key — the IR-level mirror of {@code
 * SqlQuery.JoinKeyPair}. {@code rightColumn} is always a bare physical
 * column name (the right side of a {@link Join} is always a single
 * freshly-decoded table, never previously joined). {@code leftColumn}
 * is bare only for a chain's first {@link Join} (the base table, also
 * not yet joined); for every later step, it's already alias-qualified
 * ({@code "<alias>.<column>"}), because by then the left side is a
 * prior step's {@code core.execution.HashJoinOperator} output, whose
 * columns are already renamed that way.
 */
public record JoinKey(String leftColumn, String rightColumn) {
}
