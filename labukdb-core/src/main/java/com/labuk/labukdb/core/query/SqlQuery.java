package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.JoinType;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.planner.ComparisonOp;
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
 * @param baseAlias the base ({@code table}) table's own alias — its own
 *                   name when no explicit {@code AS} alias was written,
 *                   matching real SQL semantics. Only meaningful when
 *                   {@code joins} is non-empty (unqualified single-table
 *                   queries have no alias concept); {@code null} otherwise.
 * @param limit {@code -1} means no {@code LIMIT} clause was present.
 * @param joins empty for every non-join query (every field above keeps
 *              meaning exactly what it already did, and {@code predicate}
 *              is the whole query's {@code WHERE}). Non-empty for a join
 *              of two or more tables (SQL engine Phase 3) — see {@link
 *              JoinStep}'s own javadoc for the real scope boundaries.
 *              When non-empty: {@code columns} holds fully-qualified
 *              output column names (e.g. {@code "a.service"}, matching
 *              {@link net.sf.jsqlparser.schema.Column#getFullyQualifiedName()})
 *              rather than bare names; {@code predicate} is specifically
 *              the <em>base</em> table's own share of the {@code WHERE}
 *              clause (each join step carries its own new table's share
 *              on {@link JoinStep#rightPredicate()} instead) — {@code
 *              null} when the base table had no applicable condition.
 * @param scalarSubqueries SQL engine Phase 4, increment 2: a {@code WHERE
 *              x OP (SELECT ...)} comparison against a subquery that must
 *              be run and its single value substituted before the outer
 *              plan can be built — {@code core.cli.QueryCommand}'s job,
 *              not this translator's (no {@code TableFormat} access
 *              here). Always empty by the time anything downstream of
 *              {@code QueryCommand.resolveScalarSubqueries} sees this
 *              record; every resolved value ends up merged into {@code
 *              predicate} or the matching {@link JoinStep#rightPredicate()}
 *              instead.
 */
public record SqlQuery(String table, String baseAlias, List<String> columns, Predicate predicate,
                        List<String> groupByColumns, List<AggregateSpec> aggregates,
                        List<SortKey> orderBy, long limit, List<JoinStep> joins,
                        List<ScalarSubquery> scalarSubqueries) {

    /**
     * One step of a left-deep join chain: joins the table/alias
     * accumulated by every prior step (or the base table, for the first
     * step) against {@code rightTable}. {@code keys} pairs are physical,
     * unqualified column names (the alias is a SQL-level concept only —
     * see {@code SqlQueryTranslator} and {@code core.logical}'s own
     * javadoc for why it never reaches the physical reader).
     *
     * {@code rightPredicate} is this step's new table's own share of the
     * {@code WHERE} clause, already determined safe to evaluate before
     * this join (see {@code SqlQueryTranslator#classifyPredicateSafety} for
     * the exact rule — a predicate on the non-preserved side of an
     * outer join, or on either side of a {@code FULL} join, is rejected
     * at translation time instead, never silently pushed somewhere
     * unsafe). {@code null} when that table had no applicable condition.
     *
     * {@code type} is exactly as parsed — {@code RIGHT} included. It
     * reaches {@code core.logical.Join#type()} unchanged, too; {@code
     * RIGHT} is only ever resolved at the very end, inside {@code
     * core.execution.HashJoinOperator} (which delegates it to its {@code
     * LEFT} implementation with every argument swapped) — see that
     * class's own javadoc for why keeping the tree's left/right shape a
     * simple, unambiguous invariant was worth not canonicalizing earlier.
     *
     * {@code scalarSubqueries} is a scalar subquery found inside THIS
     * step's own subquery filter — only possible for a {@code SEMI}/
     * {@code ANTI} step built from {@code IN}/{@code EXISTS} (see {@code
     * SqlQueryTranslator#splitSubqueryWhere}), e.g. {@code EXISTS (SELECT
     * 1 FROM b WHERE b.id = a.id AND b.x = (SELECT MAX(c.y) FROM c))} —
     * scoped to {@code rightAlias} as its own "outer" table. Always empty
     * for an explicit {@code JOIN} step, and always empty by the time
     * anything downstream of {@code core.cli.QueryCommand.resolveScalarSubqueries}
     * sees this record, same convention as {@link SqlQuery#scalarSubqueries()}.
     */
    public record JoinStep(JoinType type, String rightTable, String rightAlias,
                            List<JoinKeyPair> keys, Predicate rightPredicate,
                            List<ScalarSubquery> scalarSubqueries) {
    }

    /**
     * One {@code ON a.x = b.y} equality. {@code rightColumn} is always a
     * bare physical column name (this step's new table has never been
     * joined before). {@code leftColumn} is also bare, but {@code
     * leftTableAlias} names which already-known table (the base table,
     * or any earlier step's new table) it actually belongs to — needed
     * because a 3+-way join's step can key against any table introduced
     * so far, not just the one immediately before it. See {@code
     * core.logical.LogicalPlanBuilder}'s own javadoc for how this
     * resolves into the bare-vs-alias-qualified name {@code
     * core.execution.HashJoinOperator} actually looks up.
     */
    public record JoinKeyPair(String leftTableAlias, String leftColumn, String rightColumn) {
    }

    /**
     * {@code outerTableAlias.outerColumn OP (SELECT ...)}. {@code
     * innerQuery} is a fully ordinary, already-translated {@link
     * SqlQuery} — translated with zero awareness of the outer query's
     * aliases, so a correlated reference (a column that doesn't belong
     * to the subquery's own table) fails {@code innerQuery}'s own
     * translation with the same "doesn't match any known table" error
     * every other path already throws, not a special case here.
     * {@code innerQuery} may itself have {@code scalarSubqueries} of its
     * own (arbitrary nesting) — {@code QueryCommand.resolveScalarSubqueries}
     * handles that recursively, not this record.
     */
    public record ScalarSubquery(String outerTableAlias, String outerColumn, ComparisonOp op, SqlQuery innerQuery) {
    }
}
