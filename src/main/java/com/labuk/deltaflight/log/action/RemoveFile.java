package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A file leaving the table's live set as of this commit.
 *
 * This does NOT mean the physical Parquet file is gone — it means "don't
 * include this file in any snapshot from this version onward." The bytes
 * stay on disk until a VACUUM removes them, which is exactly what makes
 * time travel to an earlier version possible: an older snapshot can still
 * legally read a file that's been "removed" in a later one.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RemoveFile {

    @JsonProperty("path")
    private String path;

    @JsonProperty("deletionTimestamp")
    private Long deletionTimestamp;

    @JsonProperty("dataChange")
    private boolean dataChange;

    public String getPath() {
        return path;
    }

    public Long getDeletionTimestamp() {
        return deletionTimestamp;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    @Override
    public String toString() {
        return "RemoveFile{path='" + path + "'}";
    }
}
