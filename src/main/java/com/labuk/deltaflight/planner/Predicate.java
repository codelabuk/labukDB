package com.labuk.deltaflight.planner;

import java.util.List;

/**
 * A filter expression: comparisons on columns, combined with AND/OR.
 *
 * Deliberately does NOT support a generic NOT. Range-based pruning (partition
 * values, file stats, row-group stats) needs to answer "could any row in
 * [min, max] satisfy this?" — that's tractable for direct comparisons
 * (EQ/LT/LTE/GT/GTE) and for AND/OR of them, but negating a comparison is
 * just the opposite comparison (NOT(x &gt; 5) is x &lt;= 5), so callers get
 * the same expressiveness by choosing the right {@link ComparisonOp}
 * directly, without this class needing to handle NOT specially at every
 * evaluation site.
 */
public interface Predicate {

    static Predicate compare(String column, ComparisonOp op, Comparable<?> literal) {
        return new Comparison(column, op, literal);
    }

    static Predicate and(Predicate... children) {
        return new And(List.of(children));
    }

    static Predicate or(Predicate... children) {
        return new Or(List.of(children));
    }

    static Predicate in(String column, List<Comparable<?>> literals) {
        return new In(column, literals);
    }

    static Predicate like(String column, String pattern) {
        return new Like(column, pattern);
    }

    final class In implements Predicate {
        public final String column;
        public final List<Comparable<?>> literals;

        In(String column, List<Comparable<?>> literals) {
            this.column = column;
            this.literals = literals;
        }

        @Override
        public String toString() {
            return column + " IN " + literals;
        }
    }

    /** SQL-style LIKE: {@code %} matches any run of characters, {@code _} matches exactly one. Case-sensitive. */
    final class Like implements Predicate {
        public final String column;
        public final String pattern;

        Like(String column, String pattern) {
            this.column = column;
            this.pattern = pattern;
        }

        @Override
        public String toString() {
            return column + " LIKE '" + pattern + "'";
        }
    }

    final class Comparison implements Predicate {
        public final String column;
        public final ComparisonOp op;
        public final Comparable<?> literal;

        Comparison(String column, ComparisonOp op, Comparable<?> literal) {
            this.column = column;
            this.op = op;
            this.literal = literal;
        }

        @Override
        public String toString() {
            return column + " " + op + " " + literal;
        }
    }

    final class And implements Predicate {
        public final List<Predicate> children;

        And(List<Predicate> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "(" + children.stream().map(Object::toString)
                    .reduce((a, b) -> a + " AND " + b).orElse("") + ")";
        }
    }

    final class Or implements Predicate {
        public final List<Predicate> children;

        Or(List<Predicate> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "(" + children.stream().map(Object::toString)
                    .reduce((a, b) -> a + " OR " + b).orElse("") + ")";
        }
    }
}
