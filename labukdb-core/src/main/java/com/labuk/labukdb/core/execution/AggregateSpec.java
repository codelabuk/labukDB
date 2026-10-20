package com.labuk.labukdb.core.execution;

/** One {@code GROUP BY} aggregate to compute: {@code function(column) AS outputName}. */
public record AggregateSpec(String column, AggregateFunction function, String outputName) {
}
