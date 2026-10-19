package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A deliberately small filter syntax for the CLI — not SQL, not trying to
 * be. Supports:
 * <pre>
 *   level=ERROR
 *   amount>2
 *   level='ERROR' AND amount>2       (AND only — no OR, no parentheses)
 *   level IN (ERROR,WARN)
 *   log_date LIKE '2026-07%'
 * </pre>
 * Values are inferred as Long, then Double, then String, unless wrapped in
 * single quotes to force a string explicitly (useful when a value happens
 * to look numeric, e.g. {@code region='001'}).
 *
 * A real query language (OR, parentheses, functions) is exactly what
 * {@code PredicateCodec}'s JSON wire format already supports — this parser
 * exists only to make the common case typeable at a terminal, not to
 * replace the wire format's expressiveness.
 */
final class SimpleFilterParser {

    private static final Pattern IN_PATTERN = Pattern.compile("(?i)^(\\S+)\\s+IN\\s*\\((.+)\\)$");
    private static final Pattern LIKE_PATTERN = Pattern.compile("(?i)^(\\S+)\\s+LIKE\\s+(.+)$");
    private static final Pattern CMP_PATTERN = Pattern.compile("^(\\S+)\\s*(>=|<=|!=|=|>|<)\\s*(.+)$");

    private SimpleFilterParser() {
    }

    static Predicate parse(String expr) {
        if (expr == null || expr.isBlank()) {
            return null;
        }
        String[] parts = expr.split("(?i)\\s+AND\\s+");
        List<Predicate> conditions = new ArrayList<>();
        for (String part : parts) {
            conditions.add(parseCondition(part.trim()));
        }
        return conditions.size() == 1 ? conditions.get(0) : Predicate.and(conditions.toArray(new Predicate[0]));
    }

    private static Predicate parseCondition(String cond) {
        Matcher inMatch = IN_PATTERN.matcher(cond);
        if (inMatch.matches()) {
            List<Comparable<?>> literals = new ArrayList<>();
            for (String v : inMatch.group(2).split(",")) {
                literals.add(parseValue(v.trim()));
            }
            return Predicate.in(inMatch.group(1), literals);
        }

        Matcher likeMatch = LIKE_PATTERN.matcher(cond);
        if (likeMatch.matches()) {
            return Predicate.like(likeMatch.group(1), stripQuotes(likeMatch.group(2).trim()));
        }

        Matcher cmpMatch = CMP_PATTERN.matcher(cond);
        if (cmpMatch.matches()) {
            ComparisonOp op;
            switch (cmpMatch.group(2)) {
                case "=": op = ComparisonOp.EQ; break;
                case "!=": op = ComparisonOp.NEQ; break;
                case ">": op = ComparisonOp.GT; break;
                case ">=": op = ComparisonOp.GTE; break;
                case "<": op = ComparisonOp.LT; break;
                case "<=": op = ComparisonOp.LTE; break;
                default: throw new IllegalArgumentException("Unreachable — regex only matches known operators");
            }
            return Predicate.compare(cmpMatch.group(1), op, parseValue(cmpMatch.group(3).trim()));
        }

        throw new IllegalArgumentException("Could not parse filter condition: '" + cond + "'. Supported forms: "
                + "col=val, col!=val, col>val, col>=val, col<val, col<=val, col IN (v1,v2), col LIKE pattern. "
                + "Only AND joins multiple conditions.");
    }

    private static Comparable<?> parseValue(String raw) {
        String stripped = stripQuotes(raw);
        if (!stripped.equals(raw)) {
            return stripped; // explicitly quoted — force string, skip numeric inference
        }
        try {
            return Long.parseLong(stripped);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Double.parseDouble(stripped);
        } catch (NumberFormatException ignored) {
        }
        return stripped;
    }

    private static String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
