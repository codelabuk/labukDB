package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Declares the minimum protocol version a reader/writer must support to
 * safely interact with the table. A conformant engine is supposed to
 * refuse to read a table whose minReaderVersion it doesn't support, rather
 * than silently reading a subset of the data and getting it wrong.
 *
 * This engine only targets basic Delta tables (append/overwrite, no
 * column mapping, no deletion vectors) — SnapshotManager will surface this
 * value so callers can decide whether to trust the read, rather than the
 * reader silently pretending compatibility it doesn't have.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProtocolAction {

    @JsonProperty("minReaderVersion")
    private int minReaderVersion;

    @JsonProperty("minWriterVersion")
    private int minWriterVersion;

    public ProtocolAction() {
    }

    public ProtocolAction(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    public int getMinReaderVersion() {
        return minReaderVersion;
    }

    public int getMinWriterVersion() {
        return minWriterVersion;
    }
}
