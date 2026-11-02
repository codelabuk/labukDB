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
 * @param join {@code null} for every non-join query (every field above
 *             keeps meaning exactly what it already did). Non-null only
 *             for a two-table {@code INNER} equi-join (SQL engine Phase
 *             3, increment 1) — see {@link JoinClause}'s own javadoc for
 *             the real scope boundaries. When non-null, {@code columns}
 *             holds fully-qualified output column names (e.g.
 *             {@code "a.service"}, matching {@link
 *             net.sf.jsqlparser.schema.Column#getFullyQualifiedName()})
 *             rather than bare names, and {@code predicate}/
 *             {@code groupByColumns}/{@code aggregates}/{@code orderBy}/
 *             {@code limit} are always empty/{@code null}/{@code -1} —
 *             {@code GROUP BY}/aggregates/{@code ORDER BY}/{@code LIMIT}
 *             combined with a join are rejected by the translator, not
 *             silently dropped here.
 */
public record SqlQuery(String table, List<String> columns, Predicate predicate,
                        List<String> groupByColumns, List<AggregateSpec> aggregates,
                        List<SortKey> orderBy, long limit, JoinClause join) {

    /**
     * A two-table {@code INNER} equi-join — {@code leftAlias}/{@code
     * rightAlias} are the table's own name when no explicit {@code AS}
     * alias was written (matching real SQL semantics: the unaliased
     * table name is itself the qualifier other columns reference it by).
     * {@code keys} pairs are physical, unqualified column names (the
     * alias is a SQL-level concept only — see {@code SqlQueryTranslator}
     * and {@code core.logical}'s own javadoc for why it never reaches
     * the physical reader). {@code leftPredicate}/{@code rightPredicate}
     * are each side's own share of the {@code WHERE} clause, already
     * split by which table's columns they reference — {@code null} when
     * that side had no applicable condition.
     */
    public record JoinClause(String leftAlias, String rightTable, String rightAlias,
                              List<JoinKeyPair> keys, Predicate leftPredicate, Predicate rightPredicate) {
    }

    /** One {@code ON a.x = b.y} equality — physical, unqualified column names. */
    public record JoinKeyPair(String leftColumn, String rightColumn) {
    }
}
