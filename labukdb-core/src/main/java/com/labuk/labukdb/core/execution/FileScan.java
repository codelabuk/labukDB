package com.labuk.labukdb.core.execution;

import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.storage.TableLocation;

import java.util.Map;

/**
 * One surviving file's scan parameters — what {@link CombinedQueryExecutor}
 * needs to decode it: where it is, its already-simplified residual
 * predicate (partition-column clauses already folded to constants, same
 * convention every other per-file scan in this project uses), and its
 * partition values (for reconstructing partition columns as constants
 * during decode).
 */
public record FileScan(TableLocation dataFile, Predicate residualPredicate, Map<String, String> partitionValues) {
}
