package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Free-form operational info about a commit (timestamp, operation name,
 * operation parameters, engine info). None of this is required to answer a
 * query correctly — it's not part of what defines the live file set or the
 * schema — so it's captured generically rather than modeled field by field.
 * Useful later for an audit trail of what wrote what and when.
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public class CommitInfoAction {

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("operation")
    private String operation;

    private final Map<String, Object> other = new HashMap<>();

    @JsonAnySetter
    public void setOther(String key, Object value) {
        other.put(key, value);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getOperation() {
        return operation;
    }

    public Map<String, Object> getOther() {
        return other;
    }
}
