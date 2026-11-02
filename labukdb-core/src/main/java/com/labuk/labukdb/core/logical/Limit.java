package com.labuk.labukdb.core.logical;

/** A {@code LIMIT} node. */
public record Limit(LogicalPlan input, long limit) implements LogicalPlan {
}
