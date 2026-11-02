package com.labuk.labukdb.core.logical;

/**
 * One {@code ON a.x = b.y} equality key, physical/unqualified column
 * names on each side — the IR-level mirror of {@code
 * SqlQuery.JoinKeyPair}.
 */
public record JoinKey(String leftColumn, String rightColumn) {
}
