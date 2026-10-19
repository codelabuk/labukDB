package com.labuk.labukdb.core.format;

import java.time.Instant;

/**
 * A table format's read path, normalized away from any one format's own
 * action/manifest model — {@link DeltaTableFormat} wraps
 * {@code log.SnapshotManager} today; an eventual Iceberg implementation
 * would wrap {@code iceberg-core}'s manifest reading instead, without
 * either one leaking its own model into callers.
 *
 * <b>Status, stated plainly:</b> this interface exists and is tested
 * against its one real implementation ({@link DeltaTableFormat}), proving
 * the shape is at least plausible — but {@code planner}/{@code flight}/
 * {@code cli} still consume {@code SnapshotManager} and {@code AddFile}
 * directly, not this interface. Migrating them is deliberately deferred
 * until there's a second implementation to validate this shape against;
 * doing that migration now, on the strength of one implementation, would
 * be designing for a guess rather than a proven need. See
 * {@code docs/labukdb-v2-architecture.md} for the full plan.
 */
public interface TableFormat {

    NormalizedSnapshot latest();

    NormalizedSnapshot atVersion(long version);

    NormalizedSnapshot atTimestamp(Instant timestamp);
}
