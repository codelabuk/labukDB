package com.labuk.deltaflight.planner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.deltaflight.log.action.AddFile;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Layer 2, step 2: eliminate whole files using the {@code stats} JSON blob
 * an {@link AddFile} optionally carries — {@code minValues}/{@code maxValues}
 * per column, written by the ingesting engine (Spark records this for the
 * first 32 columns by default). Still no file opened; this reads only the
 * log, same as {@link PartitionPruner}, just against a richer set of columns
 * than partition values cover.
 *
 * If a file has no stats at all, or no stats for the column a predicate
 * references, this can't prune it — it's kept, and {@link
 * com.labuk.deltaflight.decode.RowGroupPruner} gets the next chance once the
 * file is actually opened.
 */
public final class FileStatsPruner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FileStatsPruner() {
    }

    public static List<AddFile> prune(Predicate predicate, Collection<AddFile> files) {
        if (predicate == null) {
            return List.copyOf(files);
        }
        return files.stream()
                .filter(f -> mightMatch(predicate, f))
                .collect(Collectors.toList());
    }

    static boolean mightMatch(Predicate predicate, AddFile file) {
        if (file.getStats() == null) {
            return true;
        }
        JsonNode stats;
        try {
            stats = MAPPER.readTree(file.getStats());
        } catch (Exception e) {
            return true; // malformed stats — fail open, don't let a parse hiccup drop data
        }
        return evaluate(predicate, stats);
    }

    private static boolean evaluate(Predicate predicate, JsonNode stats) {
        if (predicate instanceof Predicate.And and) {
            for (Predicate child : and.children) {
                if (!evaluate(child, stats)) {
                    return false;
                }
            }
            return true;
        }
        if (predicate instanceof Predicate.Or or) {
            for (Predicate child : or.children) {
                if (evaluate(child, stats)) {
                    return true;
                }
            }
            return false;
        }
        if (predicate instanceof Predicate.Comparison cmp) {
            JsonNode min = section(stats, "minValues", cmp.column);
            JsonNode max = section(stats, "maxValues", cmp.column);
            if (min == null || max == null) {
                return true;
            }
            return PredicateEvaluation.mightMatchRange(toObject(min), toObject(max), cmp.op, cmp.literal);
        }
        return true;
    }

    private static JsonNode section(JsonNode stats, String sectionName, String column) {
        JsonNode section = stats.get(sectionName);
        if (section == null) {
            return null;
        }
        JsonNode value = section.get(column);
        return (value == null || value.isMissingNode() || value.isNull()) ? null : value;
    }

    private static Object toObject(JsonNode node) {
        if (node.isTextual()) return node.asText();
        if (node.isIntegralNumber()) return node.asLong();
        if (node.isFloatingPointNumber()) return node.asDouble();
        if (node.isBoolean()) return node.asBoolean();
        return node.asText();
    }
}
