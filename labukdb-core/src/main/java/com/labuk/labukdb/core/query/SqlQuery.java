package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.planner.Predicate;

import java.util.List;

/**
 * The result of translating a SQL {@code SELECT} statement — exactly the
 * structures the engine already executes correctly ({@link Predicate},
 * {@link AggregateSpec}, {@link SortKey}), so a caller hands this
 * straight to {@code QueryPlanner}/{@code CombinedQueryExecutor} the same
 * way it would a hand-built request.
 *
 * {@code table} is deliberately just the raw {@code FROM}-clause name —
 * resolving it (a catalog name vs. a raw path) is the caller's job, not
 * this translator's, matching how {@code --table} already works on the
 * CLI.
 *
 * @param limit {@code -1} means no {@code LIMIT} clause was present.
 */
public record SqlQuery(String table, List<String> columns, Predicate predicate,
                        List<String> groupByColumns, List<AggregateSpec> aggregates,
                        List<SortKey> orderBy, long limit) {
}
