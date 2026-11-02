package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
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
import java.util.LinkedHashSet;
import java.util.List;
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
 * <b>{@code JOIN}, SQL engine Phase 3 increment 1 — two-table {@code
 * INNER} equi-join only:</b> exactly one {@code JOIN}, {@code INNER}
 * only (no {@code LEFT}/{@code RIGHT}/{@code FULL}/{@code CROSS}),
 * {@code ON} must be one or more {@code AND}-combined column-to-column
 * equalities (no non-equi conditions), every column referenced anywhere
 * ({@code SELECT}/{@code WHERE}/{@code ON}) must be table-qualified, and
 * {@code GROUP BY}/aggregates/{@code ORDER BY}/{@code LIMIT} combined
 * with a join are rejected (not yet supported together). {@code WHERE}
 * must be per-side-splittable — each top-level {@code AND}-ed condition
 * must reference columns from exactly one of the two joined tables; a
 * condition spanning both belongs in {@code JOIN ... ON}, not
 * {@code WHERE}, and is rejected with a clear error (see {@link
 * SqlQuery.JoinClause}'s own javadoc for why: evaluating a cross-table
 * {@code WHERE} condition after the join would need a "{@code Predicate}
 * against an already-decoded Arrow row" evaluator that doesn't exist
 * yet, a separate piece of work).
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

        return new SqlQuery(table.getName(), effectiveColumns, predicate, groupByColumns, aggregates, orderBy, limit, null);
    }

    /**
     * SQL engine Phase 3, increment 1 — see this class's own javadoc for
     * the exact scope boundaries. Reached only once {@code translate}
     * has already confirmed a non-empty {@code JOIN} list.
     */
    private static SqlQuery translateJoin(PlainSelect plainSelect, Table leftTable, List<Join> joins) {
        if (joins.size() != 1) {
            throw new IllegalArgumentException(
                    "Only a single JOIN (two tables) is supported, found " + joins.size());
        }
        Join join = joins.get(0);
        if (!join.isInnerJoin()) {
            throw new IllegalArgumentException(
                    "Only INNER JOIN is supported (no LEFT/RIGHT/FULL/CROSS/OUTER): " + join);
        }
        if (!(join.getRightItem() instanceof Table rightTable)) {
            throw new IllegalArgumentException(
                    "JOIN's right side must be a plain table reference, not a subquery: " + join.getRightItem());
        }

        if (plainSelect.getGroupBy() != null
                || (plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty())
                || plainSelect.getLimit() != null
                || hasAggregateSelectItem(plainSelect)) {
            throw new IllegalArgumentException(
                    "GROUP BY/aggregates/ORDER BY/LIMIT combined with JOIN is not supported yet");
        }

        String leftAlias = aliasOrName(leftTable);
        String rightAlias = aliasOrName(rightTable);

        List<String> joinColumns = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (!(expr instanceof Column column) || column.getTable() == null) {
                throw new IllegalArgumentException(
                        "Every SELECT column must be table-qualified when JOIN is present, e.g. "
                                + leftAlias + ".col: " + expr);
            }
            joinColumns.add(column.getFullyQualifiedName());
        }

        List<SqlQuery.JoinKeyPair> keys = toJoinKeys(join.getOnExpression(), leftAlias, rightAlias);

        Predicate leftPredicate = null;
        Predicate rightPredicate = null;
        if (plainSelect.getWhere() != null) {
            Expression[] split = splitJoinWhere(plainSelect.getWhere(), leftAlias, rightAlias);
            leftPredicate = split[0] == null ? null : toPredicate(split[0]);
            rightPredicate = split[1] == null ? null : toPredicate(split[1]);
        }

        SqlQuery.JoinClause joinClause = new SqlQuery.JoinClause(
                leftAlias, rightTable.getName(), rightAlias, keys, leftPredicate, rightPredicate);
        return new SqlQuery(leftTable.getName(), joinColumns, null, List.of(), List.of(), List.of(), -1, joinClause);
    }

    private static boolean hasAggregateSelectItem(PlainSelect plainSelect) {
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            if (item.getExpression() instanceof Function) {
                return true;
            }
        }
        return false;
    }

    private static String aliasOrName(Table table) {
        return table.getAlias() != null ? table.getAlias().getName() : table.getName();
    }

    /** {@code ON a.x = b.y [AND a.y2 = b.y2 ...]} — no non-equi conditions, no OR. */
    private static List<SqlQuery.JoinKeyPair> toJoinKeys(Expression onExpr, String leftAlias, String rightAlias) {
        if (onExpr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            return toJoinKeys(grouped.get(0), leftAlias, rightAlias);
        }
        if (onExpr instanceof AndExpression and) {
            List<SqlQuery.JoinKeyPair> result = new ArrayList<>(toJoinKeys(and.getLeftExpression(), leftAlias, rightAlias));
            result.addAll(toJoinKeys(and.getRightExpression(), leftAlias, rightAlias));
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
        if (leftAlias.equals(leftColTable) && rightAlias.equals(rightColTable)) {
            return List.of(new SqlQuery.JoinKeyPair(leftCol.getColumnName(), rightCol.getColumnName()));
        }
        if (rightAlias.equals(leftColTable) && leftAlias.equals(rightColTable)) {
            return List.of(new SqlQuery.JoinKeyPair(rightCol.getColumnName(), leftCol.getColumnName()));
        }
        throw new IllegalArgumentException("JOIN ON columns must be qualified with the two joined tables ("
                + leftAlias + ", " + rightAlias + "): " + eq);
    }

    /**
     * Splits a {@code WHERE} expression's top-level {@code AND} tree into
     * each side's own share, operating on the raw {@code jsqlparser}
     * {@link Expression} tree (not yet a {@link Predicate}) so each
     * leaf's real table qualifier is still visible — {@link #toPredicate}
     * (called separately on each half afterward) already strips
     * qualifiers via {@code Column.getColumnName()}, which is exactly
     * right for the physical column name but means routing must happen
     * before that conversion, not after.
     *
     * @return a 2-element array, {@code [leftExpression, rightExpression]} — either may be {@code null}
     */
    private static Expression[] splitJoinWhere(Expression expr, String leftAlias, String rightAlias) {
        if (expr instanceof ParenthesedExpressionList<?> grouped && grouped.size() == 1) {
            return splitJoinWhere(grouped.get(0), leftAlias, rightAlias);
        }
        if (expr instanceof AndExpression and) {
            Expression[] left = splitJoinWhere(and.getLeftExpression(), leftAlias, rightAlias);
            Expression[] right = splitJoinWhere(and.getRightExpression(), leftAlias, rightAlias);
            return new Expression[] {andTogether(left[0], right[0]), andTogether(left[1], right[1])};
        }

        Set<String> tables = new LinkedHashSet<>();
        collectColumnTables(expr, tables);
        if (tables.size() != 1) {
            throw new IllegalArgumentException("WHERE condition must reference columns from exactly one joined "
                    + "table (found " + tables + "); a condition spanning both tables belongs in JOIN ... ON, "
                    + "not WHERE: " + expr);
        }
        String side = tables.iterator().next();
        if (side == null) {
            throw new IllegalArgumentException(
                    "Every WHERE column must be table-qualified when JOIN is present: " + expr);
        }
        if (leftAlias.equals(side)) {
            return new Expression[] {expr, null};
        }
        if (rightAlias.equals(side)) {
            return new Expression[] {null, expr};
        }
        throw new IllegalArgumentException("WHERE column's table qualifier '" + side
                + "' doesn't match either joined table (" + leftAlias + ", " + rightAlias + "): " + expr);
    }

    private static Expression andTogether(Expression a, Expression b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return new AndExpression(a, b);
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
