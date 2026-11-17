package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.TableCreator;
import com.labuk.labukdb.core.log.action.AddFile;
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
 * Closes the real gap item #10 named: {@code AddFile.stats} used to always
 * be {@code null} on write, which meant {@link FileStatsPruner} never
 * actually pruned anything for a Delta table this project itself wrote —
 * every file always "might match," regardless of its real value range.
 * Now that {@code CommitWriter} calls {@code FileStatsCollector} for real,
 * this proves the whole chain end to end: two files with genuinely
 * non-overlapping value ranges, written through the real commit path, and
 * a predicate that should only ever match one of them — the other must be
 * skipped by {@link QueryPlanner#plan}, not just "correctly scanned and
 * then filtered," which a unit test against a hand-built {@code AddFile}
 * (stats fabricated by the test itself) can't prove by itself — this one
 * goes through the real write path end to end instead.
 */
class FileStatsPruningRealWriteIntegrationTest {

    @Test
    void predicateOutsideOneFilesRange_thatFileIsSkippedByStatsPruning_notJustFiltered(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        new TableCreator().createTable(location, new Schema(List.of(amount)), List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            appendAmounts(location, allocator, new long[] {1, 2, 3, 4, 5});     // file A: amount 1..5
            appendAmounts(location, allocator, new long[] {100, 101, 102});     // file B: amount 100..102
        }

        Snapshot snapshot = SnapshotManager.forLocation(location).latest();
        assertEquals(2, snapshot.getActiveFileCount(), "two separate files, one per appendBatch call");

        // amount > 50 can only ever match file B -- file A's real max (5) is
        // well below 50, so FileStatsPruner must skip it using the stats
        // CommitWriter just wrote, before any file is opened.
        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 50L);
        ScanPlan plan = new QueryPlanner().plan(snapshot, predicate, List.of("amount"));

        assertEquals(1, plan.getFilesSkipped(), "file A must be skipped by real file-stats pruning, not just scanned and filtered");
        assertEquals(1, plan.getFilesToScan().size());
        AddFile survivor = plan.getFilesToScan().get(0);
        assertTrue(survivor.getStats().contains("\"minValues\":{\"amount\":100}"), survivor.getStats());
    }

    @Test
    void predicateWithinBothFilesRanges_neitherFileSkipped(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        new TableCreator().createTable(location, new Schema(List.of(amount)), List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            appendAmounts(location, allocator, new long[] {1, 2, 3});
            appendAmounts(location, allocator, new long[] {100, 101, 102});
        }

        Snapshot snapshot = SnapshotManager.forLocation(location).latest();

        // amount < 1000 is satisfiable by both files' real ranges -- neither should be skipped.
        Predicate predicate = Predicate.compare("amount", ComparisonOp.LT, 1000L);
        ScanPlan plan = new QueryPlanner().plan(snapshot, predicate, List.of("amount"));

        assertEquals(0, plan.getFilesSkipped());
        assertEquals(2, plan.getFilesToScan().size());
    }

    private void appendAmounts(TableLocation location, BufferAllocator allocator, long[] amounts) throws Exception {
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(amount));
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < amounts.length; i++) {
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(amounts.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }
}
