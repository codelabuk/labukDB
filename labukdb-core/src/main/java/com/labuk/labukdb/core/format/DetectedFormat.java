package com.labuk.labukdb.core.format;

import com.labuk.labukdb.core.storage.TableLocation;

import java.io.IOException;

/**
 * Which table format lives at a {@link TableLocation}, decided by probing
 * for the one directory each format's protocol requires at the table
 * root — {@code _delta_log/} for Delta, {@code metadata/} for Iceberg.
 * No catalog schema change needed for this: the format is detected lazily
 * at query time rather than stored alongside the location.
 *
 * This is the first real caller of {@link TableFormat}'s shape beyond its
 * own standalone tests. Two real shape mismatches validated it: Iceberg's
 * non-sequential {@code snapshotId} vs. Delta's sequential version;
 * Iceberg's {@code Schema.toString()} vs. Delta's JSON {@code schemaString}.
 */
public enum DetectedFormat {
    DELTA,
    ICEBERG;

    public static DetectedFormat detect(TableLocation location) throws IOException {
        if (location.exists("_delta_log")) {
            return DELTA;
        }
        if (location.exists("metadata")) {
            return ICEBERG;
        }
        throw new IllegalArgumentException("Not a recognized table format at " + location
                + " -- expected a _delta_log/ directory (Delta) or a metadata/ directory (Iceberg)");
    }
}
