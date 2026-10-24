package com.labuk.labukdb.flight.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Wire format for {@link FlightQueryRequest}'s {@code aggregates}/
 * {@code orderBy} fields, matched to {@link AggregateSpec}/{@link SortKey}
 * (both reused as-is from {@code core/execution/} — this is purely a JSON
 * ↔ Java translation, no new aggregate/sort logic).
 *
 * <pre>{@code
 * "aggregates": [
 *   {"function": "COUNT", "column": "id", "outputName": "cnt"},
 *   {"function": "AVG", "column": "amount", "outputName": "avg_amount"}
 * ],
 * "orderBy": [{"column": "avg_amount", "ascending": false}]
 * }</pre>
 */
public final class AggregateQueryCodec {

    private AggregateQueryCodec() {
    }

    public static List<AggregateSpec> aggregatesFromJson(JsonNode node) {
        List<AggregateSpec> result = new ArrayList<>();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return result;
        }
        for (JsonNode item : node) {
            String function = requireText(item, "function");
            String column = requireText(item, "column");
            String outputName = requireText(item, "outputName");
            result.add(new AggregateSpec(column, AggregateFunction.valueOf(function), outputName));
        }
        return result;
    }

    public static List<SortKey> orderByFromJson(JsonNode node) {
        List<SortKey> result = new ArrayList<>();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return result;
        }
        for (JsonNode item : node) {
            String column = requireText(item, "column");
            boolean ascending = !item.has("ascending") || item.get("ascending").asBoolean(true);
            result.add(new SortKey(column, ascending));
        }
        return result;
    }

    private static String requireText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Aggregate/orderBy JSON missing required field '" + field + "'");
        }
        return value.asText();
    }
}
