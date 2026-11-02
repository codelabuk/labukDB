package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.planner.Predicate;

import java.util.List;

/**
 * The leaf of every {@link LogicalPlan} tree — a reference to one table by
 * name. {@code predicate}/{@code requestedColumns} start out {@code null}
 * ("no WHERE yet"/"all columns") from {@link LogicalPlanBuilder} and get
 * filled in by {@link PushFilterIntoScan}/{@link PushProjectIntoScan}
 * during optimization. Catalog/format resolution (which table format lives
 * at this name, what its current snapshot is) is deliberately NOT part of
 * this node — that's I/O, deferred to execution time, matching exactly
 * where resolution already happens today in {@code QueryCommand.runSql}.
 */
public record TableScan(String tableName, Predicate predicate, List<String> requestedColumns) implements LogicalPlan {
}
