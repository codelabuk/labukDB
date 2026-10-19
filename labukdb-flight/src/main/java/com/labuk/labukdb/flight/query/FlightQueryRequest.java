package com.labuk.labukdb.flight.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * What a client sends, JSON-encoded, as an Arrow Flight
 * {@code FlightDescriptor}'s command bytes. Example:
 *
 * <pre>{@code
 * {
 *   "table": "logs",
 *   "columns": ["level", "message", "event_ts"],
 *   "predicate": {
 *     "type": "cmp", "column": "level", "op": "EQ",
 *     "literal": "ERROR", "literalType": "string"
 *   }
 * }
 * }</pre>
 *
 * Exactly one of {@code table} (a name registered in {@code TableCatalog})
 * or {@code tableRoot} (a raw filesystem path — the original, still
 * supported form) must be given; {@code table} is resolved first if both
 * are present.
 *
 * {@code version}/{@code timestampMillis} are mutually exclusive; if both
 * are null, the query resolves against the latest snapshot. {@code columns}
 * null or empty means "all columns." {@code predicate} null means "no
 * filter, scan everything."
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightQueryRequest {

    private String table;
    private String tableRoot;
    private Long version;
    private Long timestampMillis;
    private List<String> columns;
    private JsonNode predicate;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTableRoot() {
        return tableRoot;
    }

    public void setTableRoot(String tableRoot) {
        this.tableRoot = tableRoot;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Long getTimestampMillis() {
        return timestampMillis;
    }

    public void setTimestampMillis(Long timestampMillis) {
        this.timestampMillis = timestampMillis;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public JsonNode getPredicate() {
        return predicate;
    }

    public void setPredicate(JsonNode predicate) {
        this.predicate = predicate;
    }
}
