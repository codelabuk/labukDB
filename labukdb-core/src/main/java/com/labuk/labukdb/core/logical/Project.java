package com.labuk.labukdb.core.logical;

import java.util.List;

/**
 * The final {@code SELECT} column list. {@link LogicalPlanBuilder} always
 * wraps the whole tree in exactly one of these. {@link PushProjectIntoScan}
 * eliminates it entirely once it has pushed the real needed-column set
 * down into the {@link TableScan} — for a single-table query this is
 * always a no-op, since there is no remaining physical work once the
 * reader only decodes the needed columns.
 */
public record Project(LogicalPlan input, List<String> columns) implements LogicalPlan {
}
