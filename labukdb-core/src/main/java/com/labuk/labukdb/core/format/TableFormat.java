package com.labuk.labukdb.core.format;

import java.time.Instant;

/**
 * A table format's read path, normalized away from any one format's own
 * action/manifest model — {@link DeltaTableFormat} wraps
 * {@code log.SnapshotManager}; {@code IcebergTableFormat} (in
 * {@code labukdb-format-iceberg}) wraps {@code iceberg-core}'s manifest
 * reading instead, without either one leaking its own model into callers.
 *
 * {@code core.logical.LogicalPlanExecutor} resolves through this
 * interface, so the SQL engine (including joins) serves Delta and
 * Iceberg identically. The flag-based {@code --table} CLI path and
 * Flight's plain-scan path still consume {@code SnapshotManager}/
 * {@code AddFile} directly for Delta, unmigrated — see
 * {@code docs/ARCHITECTURE.md} for the current module layout.
 */
public interface TableFormat {

    NormalizedSnapshot latest();

    NormalizedSnapshot atVersion(long version);

    NormalizedSnapshot atTimestamp(Instant timestamp);
}
