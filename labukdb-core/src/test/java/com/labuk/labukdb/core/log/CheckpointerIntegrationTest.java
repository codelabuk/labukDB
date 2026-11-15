package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End to end: real commits (not a hand-built {@link Snapshot}) → {@link
 * Checkpointer} → a brand-new {@link SnapshotManager} instance (so there's
 * no warm in-memory cache to fall back on) cold-starting from the
 * checkpoint alone and resolving the identical table a full JSON-log replay
 * would. This is the actual proof that checkpoint writing is wired
 * correctly into this project's own read path, not just that {@link
 * CheckpointWriter} and {@link CheckpointReader} agree with each other in
 * isolation (see {@link CheckpointWriterTest} for that narrower proof).
 */
class CheckpointerIntegrationTest {

    private void appendBatch(TableLocation location, long[] ids, BufferAllocator allocator) throws Exception {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(idField));
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("id");
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    @Test
    void checkpointAtLatestVersion_thenColdStart_resolvesTheSameActiveFilesAsAFullReplay(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        new TableCreator().createTable(location, new Schema(List.of(idField)), List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            appendBatch(location, new long[] {1, 2}, allocator);
            appendBatch(location, new long[] {3}, allocator);
            appendBatch(location, new long[] {4, 5}, allocator);
        }

        // The baseline: what a full JSON replay resolves to, before any checkpoint exists.
        Snapshot beforeCheckpoint = SnapshotManager.forLocation(location).latest();
        assertEquals(3, beforeCheckpoint.getActiveFileCount(), "one file per appendBatch call");

        int rowsWritten = new Checkpointer().writeCheckpoint(location);
        assertEquals(5, rowsWritten, "3 add rows + 1 metaData + 1 protocol (TableCreator's first commit "
                + "always writes a protocol action, carried forward by SnapshotManager)");

        assertTrue(tableRoot.resolve("_delta_log/00000000000000000003.checkpoint.parquet").toFile().exists());
        assertTrue(tableRoot.resolve("_delta_log/_last_checkpoint").toFile().exists());

        // A brand-new SnapshotManager instance -- no warm cache, must cold-start
        // from the checkpoint (loadFromCheckpointIfAvailable) rather than
        // silently reusing the one above's in-memory state.
        Snapshot afterCheckpoint = SnapshotManager.forLocation(location).latest();
        assertEquals(beforeCheckpoint.getActiveFileCount(), afterCheckpoint.getActiveFileCount());
        assertEquals(beforeCheckpoint.getVersion(), afterCheckpoint.getVersion());

        Map<String, Long> beforeSizes = beforeCheckpoint.getActiveFiles().stream()
                .collect(java.util.stream.Collectors.toMap(f -> f.getPath(), f -> f.getSize()));
        for (var file : afterCheckpoint.getActiveFiles()) {
            assertEquals(beforeSizes.get(file.getPath()), file.getSize(),
                    "every active file's path+size must match the pre-checkpoint replay exactly: " + file.getPath());
        }
    }

    @Test
    void checkpointAtAnOlderVersion_laterCommitsStillApplyOnTopOfIt(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        new TableCreator().createTable(location, new Schema(List.of(idField)), List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            appendBatch(location, new long[] {1}, allocator); // version 1
            appendBatch(location, new long[] {2}, allocator); // version 2
        }

        new Checkpointer().writeCheckpoint(location, 1); // checkpoint at version 1, NOT the latest (2)

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            appendBatch(location, new long[] {3}, allocator); // version 3, committed AFTER the checkpoint exists
        }

        // Cold start must load the version-1 checkpoint as its baseline, then
        // replay versions 2 and 3 on top of it from JSON -- proving a
        // checkpoint doesn't have to be at the latest version to be useful,
        // and that applyCommit still runs correctly for everything after it.
        Snapshot latest = SnapshotManager.forLocation(location).latest();
        assertEquals(3, latest.getVersion());
        assertEquals(3, latest.getActiveFileCount());
    }
}
