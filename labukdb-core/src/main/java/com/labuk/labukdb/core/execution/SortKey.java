package com.labuk.labukdb.core.execution;

/** One {@code ORDER BY} key for {@link SortLimitOperator}. */
public record SortKey(String column, boolean ascending) {
}
