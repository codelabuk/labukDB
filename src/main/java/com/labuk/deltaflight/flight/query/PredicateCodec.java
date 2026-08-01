package com.labuk.deltaflight.flight.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.labuk.deltaflight.planner.ComparisonOp;
import com.labuk.deltaflight.planner.Predicate;

import java.util.ArrayList;
import java.util.List;

/**
 * Wire format for a {@link Predicate}, matched to the JSON shape documented
 * in {@link FlightQueryRequest}.
 *
 * <b>Literal types:</b> {@code string}, {@code long}, {@code double}, and
 * {@code timestamp} (ISO-8601, parsed to {@code Instant} — the
 * representation timestamp/INT96 columns are compared as consistently
 * across this project now, not the epoch-micros-long convention used
 * before this type existed on the wire).
 *
 * <b>Predicate types:</b> {@code cmp} (single comparison), {@code and}/
 * {@code or}, {@code in} (needs a {@code "literals"} array plus
 * {@code literalType}), {@code like} (needs a {@code "pattern"} string —
 * SQL-style, {@code %}/{@code _} wildcards, case-sensitive).
 */
public final class PredicateCodec {

    private PredicateCodec() {
    }

    public static Predicate fromJson(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        String type = requireText(node, "type");
        switch (type) {
            case "and":
                return Predicate.and(children(node));
            case "or":
                return Predicate.or(children(node));
            case "cmp":
                return Predicate.compare(requireText(node, "column"),
                        ComparisonOp.valueOf(requireText(node, "op")),
                        literal(node));
            case "in":
                return Predicate.in(requireText(node, "column"), literalList(node));
            case "like":
                return Predicate.like(requireText(node, "column"), requireText(node, "pattern"));
            default:
                throw new IllegalArgumentException("Unknown predicate type: " + type);
        }
    }

    private static List<Comparable<?>> literalList(JsonNode node) {
        String literalType = node.has("literalType") ? node.get("literalType").asText() : "string";
        JsonNode literalsNode = node.get("literals");
        if (literalsNode == null || !literalsNode.isArray()) {
            throw new IllegalArgumentException("'in' predicate requires a 'literals' array");
        }
        List<Comparable<?>> result = new ArrayList<>();
        for (JsonNode item : literalsNode) {
            result.add(literalOfType(item, literalType));
        }
        return result;
    }

    private static Predicate[] children(JsonNode node) {
        List<Predicate> result = new ArrayList<>();
        JsonNode childrenNode = node.get("children");
        if (childrenNode != null) {
            for (JsonNode child : childrenNode) {
                result.add(fromJson(child));
            }
        }
        return result.toArray(new Predicate[0]);
    }

    private static Comparable<?> literal(JsonNode node) {
        String literalType = node.has("literalType") ? node.get("literalType").asText() : "string";
        return literalOfType(node.get("literal"), literalType);
    }

    private static Comparable<?> literalOfType(JsonNode literalNode, String literalType) {
        switch (literalType) {
            case "long":
                return literalNode.asLong();
            case "double":
                return literalNode.asDouble();
            case "timestamp":
                // ISO-8601, e.g. "2026-07-17T10:00:00Z" — parsed to Instant,
                // the single representation timestamp columns (INT96) are
                // compared as throughout this project now.
                return java.time.Instant.parse(literalNode.asText());
            case "string":
            default:
                return literalNode.asText();
        }
    }

    private static String requireText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Predicate JSON missing required field '" + field + "'");
        }
        return value.asText();
    }
}
