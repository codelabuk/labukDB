package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.AggregateSpec;

import java.util.List;

/** A {@code GROUP BY} + aggregate-functions node. Reuses the existing {@link AggregateSpec} shape directly. */
public record Aggregate(LogicalPlan input, List<String> groupByColumns, List<AggregateSpec> aggregates) implements LogicalPlan {
}
