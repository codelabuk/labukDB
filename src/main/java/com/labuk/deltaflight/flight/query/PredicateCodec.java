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
 * <b>Scope:</b> literal types supported over the wire are {@code string},
 * {@code long}, and {@code double} — matching what a JSON request can carry
 * unambiguously. Predicate.compare(...) also supports {@code Instant}
 * literals (used internally for {@code atTimestamp} log resolution), but
 * there's no wire encoding for it yet; a client filtering on a timestamp
 * column today should compare against its epoch-millis long representation
 * rather than an ISO string, until this codec grows a "timestamp" literal
 * type.
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
            default:
                throw new IllegalArgumentException("Unknown predicate type: " + type);
        }
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
        JsonNode literalNode = node.get("literal");
        switch (literalType) {
            case "long":
                return literalNode.asLong();
            case "double":
                return literalNode.asDouble();
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
