package com.labuk.labukdb.flight.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * What a client sends, JSON-encoded, as the {@code FlightDescriptor}'s
 * command bytes for a {@code DoPut} call. Example:
 *
 * <pre>{@code
 * {
 *   "table": "logs",
 *   "partitionValues": {"level": "ERROR", "log_date": "2026-10-20"}
 * }
 * }</pre>
 *
 * Exactly one of {@code table} (a name registered in {@code TableCatalog})
 * or {@code tableRoot} (a raw path) must be given — same rule
 * {@link FlightQueryRequest} uses. {@code partitionValues} is required:
 * {@code WriteBuffer} buffers for one table AND one fixed partition
 * combination at a time (see its javadoc), so a put always targets
 * exactly one partition, not a mix.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightWriteRequest {

    private String table;
    private String tableRoot;
    private Map<String, String> partitionValues;

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

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(Map<String, String> partitionValues) {
        this.partitionValues = partitionValues;
    }
}
