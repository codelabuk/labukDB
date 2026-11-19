package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The realistic scenario {@link OrphanFileCleaner} exists for, built
 * through the real write path rather than a hand-written fixture: a
 * genuinely committed table (via {@code TableCreator}/{@code
 * CommitWriter}), plus one Parquet file written directly via {@link
 * ArrowToParquetWriter} with no matching commit at all -- exactly what
 * {@code CommitWriter#encodeToNewFile} leaves behind if the process dies
 * between writing the data file and committing its {@code add} action.
 */
class OrphanFileCleanerRealWriteIntegrationTest {

    @Test
    void aFileWrittenButNeverCommitted_isTheOnlyOneCleanedUp(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(amount));
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
                batch.allocateNew();
                ((BigIntVector) batch.getVector("amount")).setSafe(0, 1L);
                batch.setRowCount(1);
                CommitWriter.forLocation(location).appendBatch(batch, Map.of());
            }

            // Simulate a crash: write a real Parquet file the same way
            // CommitWriter.encodeToNewFile does internally, but never commit it.
            try (VectorSchemaRoot uncommittedBatch = VectorSchemaRoot.create(schema, allocator)) {
                uncommittedBatch.allocateNew();
                ((BigIntVector) uncommittedBatch.getVector("amount")).setSafe(0, 999L);
                uncommittedBatch.setRowCount(1);
                new ArrowToParquetWriter().write(location.resolve("crashed-writer.parquet"), uncommittedBatch);
            }
        }

        // Back-date it past the retention window -- otherwise it reads as
        // "might be an in-flight writer," which is exactly correct caution,
        // just not what this test is proving.
        Path orphanFile = tableRoot.resolve("crashed-writer.parquet");
        Files.setLastModifiedTime(orphanFile, FileTime.fromMillis(
                System.currentTimeMillis() - Duration.ofDays(10).toMillis()));

        Snapshot before = SnapshotManager.forLocation(location).latest();
        assertEquals(1, before.getActiveFileCount(), "the uncommitted file was never logged, so it isn't part of the table at all");

        List<String> deleted = new OrphanFileCleaner().clean(location, Duration.ofDays(7));

        assertEquals(List.of("crashed-writer.parquet"), deleted);
        assertFalse(Files.exists(orphanFile));

        Snapshot after = SnapshotManager.forLocation(location).latest();
        assertEquals(1, after.getActiveFileCount(), "the real, committed file is completely unaffected");
    }
}
