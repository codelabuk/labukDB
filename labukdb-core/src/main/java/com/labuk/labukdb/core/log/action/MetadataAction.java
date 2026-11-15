package com.labuk.labukdb.core.log.action;

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

    // Defaulted, not left null -- confirmed via a real Spark 3.5.3 + Delta
    // 3.0.0 read that this is a REQUIRED field, not an optional extra:
    // Spark's own DeltaLog.buildHadoopFsRelationWithFileIndex NPEs
    // (Metadata.format() returning null) against a metaData action that
    // omits it, whether that action came from a JSON commit or a
    // checkpoint's Parquet row. This project only ever writes Parquet data
    // files, so "parquet"/no options is always correct, never configurable.
    @JsonProperty("format")
    private Format format = new Format("parquet", Map.of());

    @JsonProperty("schemaString")
    private String schemaString;

    @JsonProperty("partitionColumns")
    private List<String> partitionColumns;

    @JsonProperty("configuration")
    private Map<String, String> configuration;

    @JsonProperty("createdTime")
    private Long createdTime;

    public MetadataAction() {
    }

    public MetadataAction(String id, String schemaString, List<String> partitionColumns,
                           Map<String, String> configuration, Long createdTime) {
        this.id = id;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.createdTime = createdTime;
    }

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

    public Format getFormat() {
        return format == null ? new Format("parquet", Map.of()) : format;
    }

    /** Used by {@code CheckpointReader} to carry forward whatever a checkpoint row's own {@code format} struct actually said, rather than always assuming this project's own default. */
    public void setFormat(Format format) {
        this.format = format;
    }

    /** Delta protocol's {@code metaData.format} struct — {@code provider} + arbitrary {@code options}. This project only ever sets {@code provider="parquet"}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Format {

        @JsonProperty("provider")
        private String provider;

        @JsonProperty("options")
        private Map<String, String> options;

        /** No-arg — used by Jackson for JSON deserialization. */
        public Format() {
        }

        public Format(String provider, Map<String, String> options) {
            this.provider = provider;
            this.options = options;
        }

        public String getProvider() {
            return provider == null ? "parquet" : provider;
        }

        public Map<String, String> getOptions() {
            return options == null ? Map.of() : options;
        }
    }
}
