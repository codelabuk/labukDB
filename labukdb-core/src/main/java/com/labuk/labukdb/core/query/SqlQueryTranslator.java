package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.JoinType;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL {@code SELECT} text → the exact {@link Predicate}/{@link AggregateSpec}/
 * {@link SortKey} structures the engine already executes correctly (see
 * {@link SqlQuery}'s own javadoc). Built on {@code jsqlparser} — a real
 * SQL grammar is a well-understood, solved problem this project gains
 * nothing from re-deriving, unlike the Delta/Parquet/pruning/execution
 * mechanics the rest of the engine is deliberately built from scratch to
 * understand.
 *
 * <b>Subset supported, stated plainly:</b> {@code SELECT <col> | <AGGFUNC>(<col>)
 * AS <alias> FROM <table> [WHERE <predicate>] [GROUP BY <col>,...]
 * [ORDER BY <col> [ASC|DESC],...] [LIMIT <n>]}. Predicate:
 * {@code =}/{@code !=}/{@code <}/{@code <=}/{@code >}/{@code >=},
 * {@code AND}/{@code OR}, {@code IN (...)}, {@code LIKE} — exactly
 * {@link Predicate}'s own capability, no more (it deliberately has no
 * generic {@code NOT}, see its javadoc). Aggregate functions:
 * {@code COUNT}/{@code SUM}/{@code AVG}/{@code MIN}/{@code MAX} —
 * exactly {@link AggregateFunction}'s set; an aggregate {@code SELECT}
 * item requires an alias.
 *
 * <b>Rejected with a clear error, not silently mishandled:</b>
 * {@code NOT}/{@code NOT IN}/{@code NOT LIKE} ({@link Predicate} has no
 * generic negation), {@code IS NULL}, subqueries, anything other
 * than a plain {@code SELECT} (no {@code UNION} etc.), and
 * {@code COUNT(*)} — matches {@code AggregateOperator}'s existing,
 * pre-existing {@code COUNT(column)}-only semantics (counts non-null
 * values of a named column), not a new limitation introduced here.
 *
 * <b>{@code JOIN}, SQL engine Phase 3 — {@code INNER}/{@code LEFT}/
 * {@code RIGHT}/{@code FULL}, any number of tables:</b> {@code ON} must
 * be one or more {@code AND}-combined column-to-column equalities (no
 * non-equi conditions) relating the step's new table to some
 * already-joined one; every column referenced anywhere ({@code SELECT}/
 * {@code WHERE}/{@code ON}/{@code GROUP BY}/{@code ORDER BY}/an
 * aggregate's column argument) must be table-qualified. {@code GROUP BY}/
 * aggregates/{@code ORDER BY}/{@code LIMIT} combined with a join run
 * exactly like the single-table path — {@code AggregateOperator}/
 * {@code SortLimitOperator} on the join's combined output, unchanged.
 * Still no {@code CROSS}/{@code NATURAL}/{@code SEMI} joins, no non-equi
 * conditions, no cost-based *join order* (build/probe *side* selection
 * within an already-fixed {@code INNER} step is a separate, implemented
 * thing — see {@code HashJoinOperator}).
 * <p>
 * {@code WHERE} must be per-table-splittable — each top-level {@code
 * AND}-ed condition must reference columns from exactly one joined
 * table; a condition spanning multiple tables belongs in {@code JOIN
 * ... ON}, not {@code WHERE} (rejected with a clear error: evaluating a
 * cross-table {@code WHERE} condition after the join would need a
 * "{@code Predicate} against an already-decoded Arrow row" evaluator
 * that doesn't exist yet). A condition on a single table is also
 * rejected, separately, if that table can't be safely pre-filtered
 * before its own join — see {@link SqlQuery.JoinStep}'s javadoc and
 * {@link #classifyPredicateSafety}: the non-preserved side of a
 * {@code LEFT}/{@code RIGHT} join, or either side of a {@code FULL}
 * join, would silently change which rows get null-padded if filtered
 * first. This is a deliberate, named scope boundary, not a silent gap —
 * the fix (evaluating such a predicate after the join, against the
 * null-padded result) is deferred, not attempted incorrectly.
 *
 * <b>{@code IN} must be parenthesized when combined with {@code AND}/
 * {@code OR}:</b> confirmed against a real parse, {@code jsqlparser}'s
 * grammar lets a bare {@code col IN (...)} greedily consume a trailing
 * {@code OR}/{@code AND} as part of its own right-hand expression instead
 * of binding tighter than {@code OR} as standard SQL precedence would —
 * e.g. {@code level IN ('a') OR x LIKE 'b'} does not parse the way it
 * reads. This is rejected with a specific error naming the fix (wrap the
 * {@code IN} predicate in parentheses) rather than silently misinterpreted.
 */
public final class SqlQueryTranslator {

    private SqlQueryTranslator() {
    }

    public static SqlQuery translate(String sql) {
        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("Could not parse SQL: " + e.getMessage(), e);
        }
        if (!(statement instanceof PlainSelect plainSelect)) {
            throw new IllegalArgumentException(
                    "Only a plain SELECT statement is supported (no UNION, INSERT, UPDATE, etc.): " + sql);
        }

        FromItem fromItem = plainSelect.getFromItem();
        if (!(fromItem instanceof Table table)) {
            throw new IllegalArgumentException("FROM must be a plain table reference, not a subquery: " + fromItem);
        }

        List<Join> joins = plainSelect.getJoins();
        if (joins != null && !joins.isEmpty()) {
            return translateJoin(plainSelect, table, joins);
        }

        List<String> columns = new ArrayList<>();
        List<AggregateSpec> aggregates = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Column column) {
                columns.add(column.getColumnName());
            } else if (expr instanceof Function function) {
                aggregates.add(toAggregateSpec(function, item));
            } else {
                throw new IllegalArgumentException("Unsupported SELECT item (only plain columns and "
                        + "aggregate functions are supported): " + expr);
            }
        }

        Predicate predicate = plainSelect.getWhere() == null ? null : toPredicate(plainSelect.getWhere());

        List<String> groupByColumns = new ArrayList<>();
        if (plainSelect.getGroupBy() != null) {
            // getGroupByExpressions() returns a raw ExpressionList (no <T>
            // carried through at this call site) -- cast each element
            // explicitly rather than relying on an unchecked foreach.
            for (Object raw : plainSelect.getGroupBy().getGroupByExpressions()) {
                if (!(raw instanceof Column column)) {
                    throw new IllegalArgumentException("GROUP BY only supports plain columns: " + raw);
                }
                groupByColumns.add(column.getColumnName());
            }
        }

        List<SortKey> orderBy = new ArrayList<>();
        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement element : plainSelect.getOrderByElements()) {
                Expression expr = element.getExpression();
                if (!(expr instanceof Column column)) {
                    throw new IllegalArgumentException("ORDER BY only supports plain columns, not expressions: " + expr);
                }
                orderBy.add(new SortKey(column.getColumnName(), element.isAsc()));
            }
        }

        long limit = -1;
        if (plainSelect.getLimit() != null && plainSelect.getLimit().getRowCount() != null) {
            limit = longLiteral(plainSelect.getLimit().getRowCount());
        }

        // When aggregating, "columns" isn't a separate projection concept --
        // the output is groupByColumns ++ aggregate outputs (AggregateOperator's
        // own shape); plain SELECT items collected above are not otherwise used.
        List<String> effectiveColumns = (groupByColumns.isEmpty() && aggregates.isEmpty()) ? columns : List.of();

        return new SqlQuery(table.getName(), null, effectiveColumns, predicate, groupByColumns, aggregates, orderBy, limit, List.of());
    }

    /**
     * SQL engine Phase 3 — see this class's own javadoc for the exact
     * scope boundaries. Reached only once {@code translate} has already
     * confirmed a non-empty {@code JOIN} list. Builds a left-deep chain:
     * {@code baseTable} joins {@code joins.get(0)}'s table, that
     * combined result joins {@code joins.get(1)}'s table, and so on —
     * matching exactly how {@code core.logical.LogicalPlanBuilder} folds
     * {@link SqlQuery#joins()} into nested {@code Join} nodes.
     */
    private static SqlQuery translateJoin(PlainSelect plainSelect, Table baseTable, List<Join> joins) {
        String baseAlias = aliasOrName(baseTable);

        // The order tables are introduced in -- base first, then each
        // step's new table -- is exactly the left-deep chain's own
        // "already accumulated" sequence.
        List<String> aliasesInOrder = new ArrayList<>();
        aliasesInOrder.add(baseAlias);

        List<JoinType> types = new ArrayList<>();
        List<Table> rightTables = new ArrayList<>();
        List<String> rightAliases = new ArrayList<>();
        for (Join join : joins) {
            JoinType type = joinType(join);
            if (!(join.getRightItem() instanceof Table rightTable)) {
                throw new IllegalArgumentException(
                        "JOIN's right side must be a plain table reference, not a subquery: " + join.getRightItem());
            }
            String rightAlias = aliasOrName(rightTable);
            types.add(type);
            rightTables.add(rightTable);
            rightAliases.add(rightAlias);
            aliasesInOrder.add(rightAlias);
        }
        Set<String> allAliases = new LinkedHashSet<>(aliasesInOrder);
        if (allAliases.size() != aliasesInOrder.size()) {
            throw new IllegalArgumentException("Every joined table needs a distinct alias (duplicate found): " + aliasesInOrder);
        }

        Set<String> unsafeForPredicatePushdown = classifyPredicateSafety(baseAlias, types, rightAliases);

        List<String> joinColumns = new ArrayList<>();
        List<AggregateSpec> aggregates = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Column column) {
                joinColumns.add(requireQualifiedColumnName(column, allAliases));
            } else if (expr instanceof Function function) {
                aggregates.add(toJoinAggregateSpec(function, item, allAliases));
            } else {
                throw new IllegalArgumentException("Unsupported SELECT item (only plain columns and "
                        + "aggregate functions are supported): " + expr);
            }
        }

        List<String> groupByColumns = new ArrayList<>();
        if (plainSelect.getGroupBy() != null) {
            for (Object raw : plainSelect.getGroupBy().getGroupByExpressions()) {
                if (!(raw instanceof Column column)) {
                    throw new IllegalArgumentException("GROUP BY only supports plain columns: " + raw);
                }
                groupByColumns.add(requireQualifiedColumnName(column, allAliases));
            }
        }

        List<SortKey> orderBy = new ArrayList<>();
        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement element : plainSelect.getOrderByElements()) {
                Expression expr = element.getExpression();
                if (!(expr instanceof Column column)) {
                    throw new IllegalArgumentException("ORDER BY only supports plain columns, not expressions: " + expr);
                }
                orderBy.add(new SortKey(requireQualifiedColumnName(column, allAliases), element.isAsc()));
            }
        }

        long limit = -1;
        if (plainSelect.getLimit() != null && plainSelect.getLimit().getRowCount() != null) {
            limit = longLiteral(plainSelect.getLimit().getRowCount());
        }

        // Same "effective columns" convention the non-join path already uses:
        // once GROUP BY/aggregates are present, AggregateOperator's own output
        // shape (groupByColumns ++ aggregate outputs) is the real final shape,
        // so the plain SELECT columns collected above are irrelevant.
        List<String> effectiveColumns = (groupByColumns.isEmpty() && aggregates.isEmpty()) ? joinColumns : List.of();

        // Per-table WHERE buckets, keyed by alias -- base's own bucket feeds
        // SqlQuery.predicate; every other table's feeds its introducing
        // JoinStep.rightPredicate (see assembly below).
        Map<String, Predicate> perTablePredicate = new LinkedHashMap<>();
        if (plainSelect.getWhere() != null) {
            splitJoinWhere(plainSelect.getWhere(), allAliases, unsafeForPredicatePushdown, perTablePredicate);
        }

        List<SqlQuery.JoinStep> steps = new ArrayList<>();
        for (int i = 0; i < joins.size(); i++) {
            String rightAlias = rightAliases.get(i);
            // Aliases introduced strictly before this step's own new table --
            // base, plus every earlier step's right table.
            Set<String> priorAliases = new LinkedHashSet<>(aliasesInOrder.subList(0, i + 1));
            List<SqlQuery.JoinKeyPair> keys = toJoinKeys(joins.get(i).getOnExpression(), priorAliases, rightAlias);
            steps.add(new SqlQuery.JoinStep(types.get(i), rightTables.get(i).getName(), rightAlias, keys,
                    perTablePredicate.get(rightAlias)));
        }

        return new SqlQuery(baseTable.getName(), baseAlias, effectiveColumns, perTablePredicate.get(baseAlias),
                groupByColumns, aggregates, orderBy, limit, steps);
    }

    private static JoinType joinType(Join join) {
        if (join.isInnerJoin()) {
            return JoinType.INNER;
        }
        if (join.isLeft()) {
            return JoinType.LEFT;
        }
        if (join.isRight()) {
            return JoinType.RIGHT;
        }
        if (join.isFull()) {
            return JoinType.FULL;
        }
        throw new IllegalArgumentException(
                "Only INNER/LEFT/RIGHT/FULL JOIN is supported (no CROSS/NATURAL/SEMI/OUTER without a side): " + join);
    }

    /**
     * Which aliases can't be safely pre-filtered via {@code WHERE} before
     * their own join — see {@link SqlQuery.JoinStep}'s javadoc for why.
     * Walks the left-deep chain tracking {@code accumulated} (every alias
     * introduced so far): a {@code LEFT} step taints only its own new
     * table (the non-preserved side); a {@code RIGHT} step taints every
     * already-accumulated table (the new table is the preserved side
     * here); a {@code FULL} step taints its new table AND every
     * already-accumulated one — {@code FULL} has no safely-preservable
     * side at all, since pre-filtering either input can manufacture
     * null-padded rows a correct post-join filter would have excluded
     * (verified by hand-tracing a concrete {@code FULL JOIN ... WHERE}
     * case, not assumed by analogy with {@code LEFT}). An {@code INNER}
     * step taints nothing.
     */
    private static Set<String> classifyPredicateSafety(String baseAlias, List<JoinType> types, List<String> rightAliases) {
        Set<String> unsafe = new LinkedHashSet<>();
        Set<String> accumulated = new LinkedHashSet<>();
        accumulated.add(baseAlias);
        for (int i = 0; i < types.size(); i++) {
            String newAlias = rightAliases.get(i);
            switch (types.get(i)) {
                case LEFT -> unsafe.add(newAlias);
                case RIGHT -> unsafe.addAll(accumulated);
                case FULL -> {
                    unsafe.addAll(accumulated);
                    unsafe.add(newAlias);
                }
                case INNER -> {
                    // both sides remain safe
                }
            }
            accumulated.add(newAlias);
        }
        return unsafe;
    }

    /** Same qualification rule {@link #toJoinKeys}/{@link #collectColumnTables} already enforce for ON/WHERE, reused for SELECT/GROUP BY/ORDER BY. */
    private static String requireQualifiedColumnName(Column column, Set<String> allAliases) {
        if (column.getTable() == null) {
            throw new IllegalArgumentException(
                    "Every column must be table-qualified when JOIN is present, e.g. "
                            + allAliases.iterator().next() + ".col: " + column);
        }
        String side = column.getTable().getName();
        if (!allAliases.contains(side)) {
            throw new IllegalArgumentException("Column's table qualifier '" + side
                    + "' doesn't match any joined table " + allAliases + ": " + column);
        }
        return column.getFullyQualifiedName();
    }

    /** Same as {@link #toAggregateSpec}, except its column argument must be table-qualified (a join's combined output is alias-qualified). */
    private static AggregateSpec toJoinAggregateSpec(Function function, SelectItem<?> item, Set<String> allAliases) {
        if (item.getAlias() == null) {
            throw new IllegalArgumentException("Aggregate function requires an alias, e.g. "
                    + function.getName() + "(col) AS name: " + function);
        }
        AggregateFunction aggFunction;
        try {
            aggFunction = AggregateFunction.valueOf(function.getName().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported aggregate function '" + function.getName()
                    + "' -- supported: SUM, COUNT, MIN, MAX, AVG");
        }
        ExpressionList<?> params = function.getParameters();
        if (function.isAllColumns() || (params != null && params.size() == 1 && params.get(0) instanceof AllColumns)) {
            throw new IllegalArgumentException(aggFunction + "(*) is not supported -- "
                    + aggFunction + "(column) counts non-null values of that column instead, matching SQL COUNT(column) semantics");
        }
        if (params == null || params.size() != 1 || !(params.get(0) instanceof Column column)) {
            throw new IllegalArgumentException(aggFunction + " requires exactly one plain column argument: " + function);
        }
        return new AggregateSpec(requireQualifiedColumnName(column, allAliases), aggFunction, item.getUnquotedAliasName());
    }

    private static String aliasOrName(Table table) {
        return table.getAlias() != null ? table.getAlias().getName() : table.getName();
    }

    /**
     * {@code ON a.x = b.y [AND a.y2 = b.y2 ...]} — no non-equi conditions,
     * no OR. One side of each equality must be {@code rightAlias} (this
     * step's own new table, bare column name); the other must be one of
     * {@code priorAliases} (an already-joined table — the base table, or
     * an earlier step's new table), recorded with its own alias so the
     * executor knows whether to look that column up by its bare name
     * (only true for the base table at the very first step) or by its
     * already-alias-qualified name (every other case — see {@code
     * QueryCommand.runSqlJoin}'s per-step key resolution).
     */
    private static List<SqlQuery.JoinKeyPair> toJoinKeys(Expression onExpr, Set<String> priorAliases, String rightAlias) {
        if (onExpr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            return toJoinKeys(grouped.get(0), priorAliases, rightAlias);
        }
        if (onExpr instanceof AndExpression and) {
            List<SqlQuery.JoinKeyPair> result = new ArrayList<>(toJoinKeys(and.getLeftExpression(), priorAliases, rightAlias));
            result.addAll(toJoinKeys(and.getRightExpression(), priorAliases, rightAlias));
            return result;
        }
        if (!(onExpr instanceof EqualsTo eq)) {
            throw new IllegalArgumentException(
                    "JOIN ON only supports column-to-column equality (optionally AND-combined): " + onExpr);
        }
        if (!(eq.getLeftExpression() instanceof Column leftCol) || !(eq.getRightExpression() instanceof Column rightCol)) {
            throw new IllegalArgumentException("JOIN ON equality must compare two columns, not a literal: " + eq);
        }
        String leftColTable = leftCol.getTable() == null ? null : leftCol.getTable().getName();
        String rightColTable = rightCol.getTable() == null ? null : rightCol.getTable().getName();
        if (priorAliases.contains(leftColTable) && rightAlias.equals(rightColTable)) {
            return List.of(new SqlQuery.JoinKeyPair(leftColTable, leftCol.getColumnName(), rightCol.getColumnName()));
        }
        if (priorAliases.contains(rightColTable) && rightAlias.equals(leftColTable)) {
            return List.of(new SqlQuery.JoinKeyPair(rightColTable, rightCol.getColumnName(), leftCol.getColumnName()));
        }
        throw new IllegalArgumentException("JOIN ON columns must relate this step's new table (" + rightAlias
                + ") to an already-joined table " + priorAliases + ": " + eq);
    }

    /**
     * Splits a {@code WHERE} expression's top-level {@code AND} tree into
     * each table's own share, merging directly into {@code out} (keyed
     * by alias) rather than returning anything — unlike the old two-table
     * version, there's no fixed-size result to hand back once N tables
     * are possible. Rejects (doesn't silently push) a condition on a
     * table {@code unsafeAliases} names — see {@link #classifyPredicateSafety}.
     */
    private static void splitJoinWhere(Expression expr, Set<String> allAliases, Set<String> unsafeAliases,
                                        Map<String, Predicate> out) {
        if (expr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            splitJoinWhere(grouped.get(0), allAliases, unsafeAliases, out);
            return;
        }
        if (expr instanceof AndExpression and) {
            splitJoinWhere(and.getLeftExpression(), allAliases, unsafeAliases, out);
            splitJoinWhere(and.getRightExpression(), allAliases, unsafeAliases, out);
            return;
        }

        Set<String> tables = new LinkedHashSet<>();
        collectColumnTables(expr, tables);
        if (tables.size() != 1) {
            throw new IllegalArgumentException("WHERE condition must reference columns from exactly one joined "
                    + "table (found " + tables + "); a condition spanning multiple tables belongs in JOIN ... ON, "
                    + "not WHERE: " + expr);
        }
        String side = tables.iterator().next();
        if (side == null) {
            throw new IllegalArgumentException(
                    "Every WHERE column must be table-qualified when JOIN is present: " + expr);
        }
        if (!allAliases.contains(side)) {
            throw new IllegalArgumentException("WHERE column's table qualifier '" + side
                    + "' doesn't match any joined table " + allAliases + ": " + expr);
        }
        if (unsafeAliases.contains(side)) {
            throw new IllegalArgumentException("WHERE condition on '" + side + "' can't be safely evaluated "
                    + "before its own join (it's the non-preserved side of an outer join, or either side of a "
                    + "FULL join) -- pre-filtering it would silently change which rows get null-padded. "
                    + "Not supported yet: " + expr);
        }
        out.merge(side, toPredicate(expr), Predicate::and);
    }

    /** Walks the same expression shapes {@link #toPredicate} understands, collecting each leaf's table qualifier (or {@code null} if unqualified) instead of building a {@link Predicate}. */
    private static void collectColumnTables(Expression expr, Set<String> out) {
        if (expr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            collectColumnTables(grouped.get(0), out);
        } else if (expr instanceof AndExpression and) {
            collectColumnTables(and.getLeftExpression(), out);
            collectColumnTables(and.getRightExpression(), out);
        } else if (expr instanceof OrExpression or) {
            collectColumnTables(or.getLeftExpression(), out);
            collectColumnTables(or.getRightExpression(), out);
        } else if (expr instanceof InExpression in) {
            out.add(columnTable(in.getLeftExpression()));
        } else if (expr instanceof LikeExpression like) {
            out.add(columnTable(like.getLeftExpression()));
        } else if (expr instanceof EqualsTo cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else if (expr instanceof NotEqualsTo cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else if (expr instanceof GreaterThan cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else if (expr instanceof GreaterThanEquals cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else if (expr instanceof MinorThan cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else if (expr instanceof MinorThanEquals cmp) {
            collectComparisonColumnTables(cmp.getLeftExpression(), cmp.getRightExpression(), out);
        } else {
            throw new IllegalArgumentException("Unsupported WHERE expression (no NOT, no IS NULL, no subqueries): " + expr);
        }
    }

    /**
     * The left side is always a column (same assumption {@link #comparison}
     * already makes for the non-join path) — collected unconditionally. The
     * right side is *usually* a literal, but a condition like {@code a.x =
     * b.y} is column-to-column across two tables, which must be caught here
     * (not left to {@link #toPredicate}'s later literal-only assumption) so
     * it's rejected with a clear "belongs in JOIN ... ON" error instead of a
     * confusing "unsupported literal" one from deep inside unrelated code.
     */
    private static void collectComparisonColumnTables(Expression left, Expression right, Set<String> out) {
        out.add(columnTable(left));
        if (right instanceof Column) {
            out.add(columnTable(right));
        }
    }

    private static String columnTable(Expression expr) {
        if (!(expr instanceof Column column)) {
            throw new IllegalArgumentException("Expected a plain column reference: " + expr);
        }
        return column.getTable() == null ? null : column.getTable().getName();
    }

    private static AggregateSpec toAggregateSpec(Function function, SelectItem<?> item) {
        if (item.getAlias() == null) {
            throw new IllegalArgumentException("Aggregate function requires an alias, e.g. "
                    + function.getName() + "(col) AS name: " + function);
        }
        AggregateFunction aggFunction;
        try {
            aggFunction = AggregateFunction.valueOf(function.getName().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported aggregate function '" + function.getName()
                    + "' -- supported: SUM, COUNT, MIN, MAX, AVG");
        }
        ExpressionList<?> params = function.getParameters();
        // COUNT(*) does not set Function.isAllColumns() in this jsqlparser version/grammar
        // path (confirmed via a real parse) -- it instead shows up as a single parameter
        // of type AllColumns, so that is checked explicitly rather than relying on the flag.
        if (function.isAllColumns() || (params != null && params.size() == 1 && params.get(0) instanceof AllColumns)) {
            throw new IllegalArgumentException(aggFunction + "(*) is not supported -- "
                    + aggFunction + "(column) counts non-null values of that column instead, matching SQL COUNT(column) semantics");
        }
        if (params == null || params.size() != 1 || !(params.get(0) instanceof Column column)) {
            throw new IllegalArgumentException(aggFunction + " requires exactly one plain column argument: " + function);
        }
        return new AggregateSpec(column.getColumnName(), aggFunction, item.getUnquotedAliasName());
    }

    private static Predicate toPredicate(Expression expr) {
        // A parenthesized grouping, e.g. "(a AND b)", is represented in this
        // jsqlparser version as a ParenthesedExpressionList of size 1 wrapping
        // the inner expression (confirmed via a real parse), not a dedicated
        // "Parenthesis" wrapper -- unwrap it rather than reject valid grouping.
        if (expr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            return toPredicate(grouped.get(0));
        }
        if (expr instanceof AndExpression and) {
            return Predicate.and(toPredicate(and.getLeftExpression()), toPredicate(and.getRightExpression()));
        }
        if (expr instanceof OrExpression or) {
            return Predicate.or(toPredicate(or.getLeftExpression()), toPredicate(or.getRightExpression()));
        }
        if (expr instanceof InExpression in) {
            if (in.isNot()) {
                throw new IllegalArgumentException("NOT IN is not supported (Predicate has no generic NOT)");
            }
            String column = columnName(in.getLeftExpression());
            if (!(in.getRightExpression() instanceof ExpressionList<?> list)) {
                // Confirmed via a real parse: "col IN (...) OR other" lets IN's grammar
                // production greedily consume the trailing OR/AND as its right-hand
                // expression instead of binding tighter than OR, so the right side ends
                // up as an OrExpression rather than the literal list. This is a genuine
                // jsqlparser precedence quirk, not something this translator can safely
                // repair -- the fix is explicit parentheses around the IN predicate.
                throw new IllegalArgumentException("IN requires a parenthesized literal list immediately "
                        + "after it, but found: " + in.getRightExpression().getClass().getSimpleName()
                        + ". If IN is combined with AND/OR, wrap it in parentheses, e.g. "
                        + "(" + column + " IN (...)) OR (...): " + in);
            }
            List<Comparable<?>> literals = new ArrayList<>();
            for (Expression e : list) {
                literals.add(literal(e));
            }
            return Predicate.in(column, literals);
        }
        if (expr instanceof LikeExpression like) {
            if (like.isNot()) {
                throw new IllegalArgumentException("NOT LIKE is not supported (Predicate has no generic NOT)");
            }
            return Predicate.like(columnName(like.getLeftExpression()), (String) literal(like.getRightExpression()));
        }
        if (expr instanceof EqualsTo cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.EQ, cmp.getRightExpression());
        }
        if (expr instanceof NotEqualsTo cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.NEQ, cmp.getRightExpression());
        }
        if (expr instanceof GreaterThan cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.GT, cmp.getRightExpression());
        }
        if (expr instanceof GreaterThanEquals cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.GTE, cmp.getRightExpression());
        }
        if (expr instanceof MinorThan cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.LT, cmp.getRightExpression());
        }
        if (expr instanceof MinorThanEquals cmp) {
            return comparison(cmp.getLeftExpression(), ComparisonOp.LTE, cmp.getRightExpression());
        }
        throw new IllegalArgumentException("Unsupported WHERE expression (no NOT, no IS NULL, no subqueries): " + expr);
    }

    private static Predicate comparison(Expression left, ComparisonOp op, Expression right) {
        return Predicate.compare(columnName(left), op, literal(right));
    }

    private static String columnName(Expression expr) {
        if (!(expr instanceof Column column)) {
            throw new IllegalArgumentException("Expected a plain column reference: " + expr);
        }
        return column.getColumnName();
    }

    private static Comparable<?> literal(Expression expr) {
        if (expr instanceof LongValue v) {
            return v.getValue();
        }
        if (expr instanceof DoubleValue v) {
            return v.getValue();
        }
        if (expr instanceof StringValue v) {
            return v.getValue();
        }
        throw new IllegalArgumentException("Unsupported literal (expected a number or string): " + expr);
    }

    private static long longLiteral(Expression expr) {
        if (expr instanceof LongValue v) {
            return v.getValue();
        }
        throw new IllegalArgumentException("LIMIT requires an integer literal: " + expr);
    }
}
