package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Table-level metadata: schema, partition columns, configuration. Only
 * present in commits that establish or change the schema (table creation,
 * schema evolution) — most commits are pure add/remove with no metaData
 * action at all, so the reader must carry forward the last-seen metaData
 * when reconstructing a snapshot.
 *
 * {@code schemaString} is kept raw (it's Delta's JSON-encoded Spark schema
 * representation) rather than parsed here, since interpreting it is a
 * planner/decode-layer concern, not a log-parsing concern.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataAction {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("schemaString")
    private String schemaString;

    @JsonProperty("partitionColumns")
    private List<String> partitionColumns;

    @JsonProperty("configuration")
    private Map<String, String> configuration;

    @JsonProperty("createdTime")
    private Long createdTime;

    public String getId() {
        return id;
    }

    public String getSchemaString() {
        return schemaString;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns == null ? List.of() : partitionColumns;
    }

    public Map<String, String> getConfiguration() {
        return configuration == null ? Map.of() : configuration;
    }

    public Long getCreatedTime() {
        return createdTime;
    }
}
