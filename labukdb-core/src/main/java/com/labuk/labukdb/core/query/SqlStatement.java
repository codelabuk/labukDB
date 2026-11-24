package com.labuk.labukdb.core.query;

import java.util.List;

/**
 * What {@link SqlQueryTranslator#translateAny} hands back — either one
 * ordinary query, or two-or-more independent queries combined by
 * {@code UNION}/{@code UNION ALL}. A sibling type to {@link SqlQuery},
 * not a new field on it: {@link SqlQuery} stays committed to its existing
 * one-table-or-join shape (unchanged by this type's existence), and every
 * current consumer of it ({@code LogicalPlanBuilder}, {@code QueryCommand}'s
 * single-query path) keeps working directly against {@link SqlQuery} with
 * zero changes. Only {@code core.cli.QueryCommand.runSql}, the one place
 * that needs to know a UNION happened at all, looks at this type.
 *
 * {@link SqlQueryTranslator#translate(String)} itself still rejects
 * {@code UNION} outright (unchanged) — {@link
 * SqlQueryTranslator#translateAny(String)} is the UNION-aware entry point,
 * used only where a caller is prepared to handle both cases.
 */
public sealed interface SqlStatement {

    record Single(SqlQuery query) implements SqlStatement {
    }

    /**
     * @param branches two or more independently-translated queries, each a
     *                  fully ordinary {@link SqlQuery} (own table or join
     *                  chain, own {@code WHERE}/{@code GROUP BY}/aggregates/
     *                  {@code ORDER BY}/{@code LIMIT}, own subqueries —
     *                  nothing about a branch knows it's part of a UNION)
     * @param all       {@code true} for {@code UNION ALL} (keep every row),
     *                  {@code false} for plain {@code UNION} (duplicate
     *                  rows across the combined result removed)
     */
    record Union(List<SqlQuery> branches, boolean all) implements SqlStatement {
    }
}
