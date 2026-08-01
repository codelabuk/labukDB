package com.labuk.deltaflight.planner;

import java.time.Instant;
import java.util.List;

/**
 * Two evaluation modes, both built on the same type-coercion logic:
 *
 * <ul>
 *   <li>{@link #compareExact} — "does this one actual value satisfy the
 *       comparison?" Used for exact partition values and for per-row residual
 *       filtering after decode.</li>
 *   <li>{@link #mightMatchRange} — "could ANY value in [min, max] satisfy the
 *       comparison?" Used for pruning: file-level stats, row-group stats.
 *       Returns true (keep, can't rule it out) far more often than false
 *       (skip, definitely can't match) — that asymmetry is deliberate: a
 *       pruning layer that wrongly says "skip" drops real data, a pruning
 *       layer that wrongly says "keep" just costs a wasted read. Every
 *       ambiguous case here resolves to "keep."</li>
 * </ul>
 *
 * Type coercion is driven by the literal's Java type (the caller controls
 * this by how they built the {@link Predicate}), and stats/actual values are
 * parsed to match it. Anything that can't be coerced returns "can't decide" —
 * which means true for {@code mightMatchRange} (fail open, don't prune) and
 * true for {@code compareExact} (fail open, let a later/residual layer make
 * the real decision rather than silently dropping the row here).
 */
public final class PredicateEvaluation {

    private PredicateEvaluation() {
    }

    /** True if actual equals ANY of the literals — same fail-open coercion behavior as compareExact per literal. */
    public static boolean matchesIn(Object actual, List<Comparable<?>> literals) {
        for (Comparable<?> literal : literals) {
            if (compareExact(actual, ComparisonOp.EQ, literal)) {
                return true;
            }
        }
        return false;
    }

    /** True if the [min,max] range could contain ANY of the literals. */
    public static boolean mightMatchRangeIn(Object min, Object max, List<Comparable<?>> literals) {
        for (Comparable<?> literal : literals) {
            if (mightMatchRange(min, max, ComparisonOp.EQ, literal)) {
                return true;
            }
        }
        return false;
    }

    /**
     * SQL-style LIKE — {@code %} matches any run of characters, {@code _}
     * matches exactly one, case-sensitive. Only meaningful against a
     * String actual value; a non-String actual fails open (true) for the
     * same reason coercion failures do elsewhere in this class — let a
     * layer with more context decide, rather than silently drop the row
     * here.
     */
    public static boolean matchesLike(Object actual, String pattern) {
        if (!(actual instanceof String)) {
            return true;
        }
        return ((String) actual).matches(likeToRegex(pattern));
    }

    private static String likeToRegex(String likePattern) {
        StringBuilder regex = new StringBuilder();
        for (char c : likePattern.toCharArray()) {
            switch (c) {
                case '%':
                    regex.append(".*");
                    break;
                case '_':
                    regex.append('.');
                    break;
                default:
                    if ("\\.[]{}()*+-?^$|".indexOf(c) >= 0) {
                        regex.append('\\');
                    }
                    regex.append(c);
            }
        }
        return regex.toString();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean compareExact(Object actual, ComparisonOp op, Comparable<?> literal) {
        Comparable coerced = coerce(actual, literal);
        if (coerced == null) {
            return true;
        }
        return applyOp(coerced.compareTo(literal), op);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean mightMatchRange(Object minRaw, Object maxRaw, ComparisonOp op, Comparable<?> literal) {
        Comparable min = coerce(minRaw, literal);
        Comparable max = coerce(maxRaw, literal);
        if (min == null || max == null) {
            return true;
        }
        switch (op) {
            case EQ:
                return min.compareTo(literal) <= 0 && max.compareTo(literal) >= 0;
            case LT:
                return min.compareTo(literal) < 0;
            case LTE:
                return min.compareTo(literal) <= 0;
            case GT:
                return max.compareTo(literal) > 0;
            case GTE:
                return max.compareTo(literal) >= 0;
            case NEQ:
            default:
                // A range can only fail to satisfy NEQ if min == max == literal
                // (every value in the file is the one excluded value). Anything
                // short of that degenerate case must be kept; not worth special
                // casing for how rarely it prunes anything.
                return true;
        }
    }

    private static boolean applyOp(int cmp, ComparisonOp op) {
        switch (op) {
            case EQ:
                return cmp == 0;
            case NEQ:
                return cmp != 0;
            case LT:
                return cmp < 0;
            case LTE:
                return cmp <= 0;
            case GT:
                return cmp > 0;
            case GTE:
                return cmp >= 0;
        }
        throw new IllegalStateException("unreachable — all ComparisonOp values handled above");
    }

    /**
     * Coerces {@code actual} into the same Comparable family as
     * {@code literalSample}, so {@code Comparable.compareTo} is meaningful.
     * Supports Long, Double, String, and Instant literals — the set this
     * engine's fixtures and decode layer actually produce. Anything else
     * (or a value that fails to parse) returns null, meaning "can't decide."
     */
    @SuppressWarnings("unchecked")
    private static Comparable<Object> coerce(Object actual, Comparable<?> literalSample) {
        if (actual == null) {
            return null;
        }
        try {
            if (literalSample instanceof Long) {
                if (actual instanceof String s) return (Comparable<Object>) (Comparable<?>) Long.valueOf(s.trim());
                if (actual instanceof Number n) return (Comparable<Object>) (Comparable<?>) n.longValue();
            } else if (literalSample instanceof Double) {
                if (actual instanceof String s) return (Comparable<Object>) (Comparable<?>) Double.valueOf(s.trim());
                if (actual instanceof Number n) return (Comparable<Object>) (Comparable<?>) n.doubleValue();
            } else if (literalSample instanceof Instant) {
                // Delta/Parquet stats encode timestamps as ISO-8601 strings;
                // Instant.parse handles the "...Z" form our writers use.
                if (actual instanceof String s) return (Comparable<Object>) (Comparable<?>) Instant.parse(s.trim());
                if (actual instanceof Instant i) return (Comparable<Object>) (Comparable<?>) i;
            } else if (literalSample instanceof String) {
                return (Comparable<Object>) (Comparable<?>) String.valueOf(actual);
            } else if (literalSample instanceof Boolean) {
                if (actual instanceof Boolean b) return (Comparable<Object>) (Comparable<?>) b;
                if (actual instanceof String s) return (Comparable<Object>) (Comparable<?>) Boolean.valueOf(s.trim());
            }
        } catch (RuntimeException e) {
            return null;
        }
        return null;
    }
}
