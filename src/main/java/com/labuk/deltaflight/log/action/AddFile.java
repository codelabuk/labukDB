package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * A file becoming part of the table's live set as of this commit.
 *
 * The {@code stats} field is kept as the raw JSON string rather than eagerly
 * parsed: most queries only need it for the columns they're filtering on, so
 * the planner layer parses lazily instead of paying the cost for every file
 * on every query.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AddFile {

    @JsonProperty("path")
    private String path;

    @JsonProperty("partitionValues")
    private Map<String, String> partitionValues;

    @JsonProperty("size")
    private long size;

    @JsonProperty("modificationTime")
    private long modificationTime;

    @JsonProperty("dataChange")
    private boolean dataChange;

    /** Raw JSON string, e.g. {"numRecords":100,"minValues":{...},"maxValues":{...},"nullCount":{...}} */
    @JsonProperty("stats")
    private String stats;

    /** No-arg — used by Jackson for JSON deserialization. */
    public AddFile() {
    }

    /** Used by {@code CheckpointReader} to build an instance from a decoded Parquet checkpoint row, not JSON. */
    public AddFile(String path, Map<String, String> partitionValues, long size,
                    long modificationTime, boolean dataChange, String stats) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.stats = stats;
    }

    public String getPath() {
        return path;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues == null ? Map.of() : partitionValues;
    }

    public long getSize() {
        return size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    public String getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return "AddFile{path='" + path + "', partitionValues=" + partitionValues + ", size=" + size + "}";
    }
}
