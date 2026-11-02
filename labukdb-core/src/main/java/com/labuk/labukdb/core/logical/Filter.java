package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.planner.Predicate;

/**
 * A WHERE clause. {@link LogicalPlanBuilder} only ever places this
 * directly on a {@link TableScan} (single-table queries, Phase 1's whole
 * scope); {@link PushFilterIntoScan} absorbs it into the scan and removes
 * this node during optimization.
 */
public record Filter(LogicalPlan input, Predicate condition) implements LogicalPlan {
}
