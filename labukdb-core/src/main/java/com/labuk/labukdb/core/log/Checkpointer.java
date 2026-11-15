package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;

import java.io.IOException;

/**
 * Writes a checkpoint for a Delta table — see {@link CheckpointWriter} for
 * the actual Parquet shape. Resolves the target version into a fully
 * replayed {@link Snapshot} via {@link SnapshotManager} (so a checkpoint is
 * always consistent with what JSON-log replay would produce at that same
 * version — the exact thing {@link CheckpointReaderRealSparkFixtureTest}
 * cross-checks), then hands that snapshot to {@link CheckpointWriter}.
 *
 * <b>Deliberately explicit, not automatic.</b> This project writes a
 * checkpoint only when this is called — there's no cadence policy (e.g.
 * "every 10 commits," real Delta's own default) wired into {@link
 * CommitWriter}. Deciding when to checkpoint automatically is a policy
 * choice with real trade-offs (write-amplification vs. faster cold-start
 * reads) this project doesn't need to make yet; calling this explicitly,
 * same convention {@link Vacuum}/{@link Compactor} already use for their
 * own maintenance operations, keeps that decision in the caller's hands.
 */
public final class Checkpointer {

    private final CheckpointWriter writer = new CheckpointWriter();

    /** Checkpoints the table's latest version. @return the row count written (matches {@code _last_checkpoint}'s own {@code size}). */
    public int writeCheckpoint(TableLocation location) throws IOException {
        Snapshot latest = SnapshotManager.forLocation(location).latest();
        return writeCheckpoint(location, latest.getVersion());
    }

    /** Checkpoints a specific version — must already be committed. */
    public int writeCheckpoint(TableLocation location, long version) throws IOException {
        Snapshot snapshot = SnapshotManager.forLocation(location).atVersion(version);
        LogStorage storage = LogStorage.forLocation(location);
        return writer.write(storage, snapshot);
    }
}
