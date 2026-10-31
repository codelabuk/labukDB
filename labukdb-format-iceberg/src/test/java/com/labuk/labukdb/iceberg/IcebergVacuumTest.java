package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Iceberg counterpart of {@code core.log.VacuumTest} — same safety
 * invariant proven, different mechanics underneath: Iceberg's own
 * {@code expireSnapshots()} does the live-vs-orphaned reference counting
 * natively, so the scenario to actually produce a genuinely orphaned
 * file looks different from Delta's hand-rolled {@code remove}-then-
 * "re-added since" history. Here, a real {@link IcebergCompactor} run
 * is what actually orphans a file (its two source files stop being
 * referenced by the *current* snapshot, while still referenced by now-
 * historical ones) — not a synthetic log fixture, since Iceberg's
 * metadata has no equivalent of hand-writing a stale {@code remove}
 * entry directly.
 */
class IcebergVacuumTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()));

    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String service, long amount) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        ((BigIntVector) root.getVector("amount")).setSafe(0, amount);
        root.setRowCount(1);
        return root;
    }

    @Test
    void vacuum_deletesFilesOrphanedByCompaction_keepsTheCurrentFile(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
            String path1, path2;
            try (VectorSchemaRoot b1 = buildBatch(allocator, "auth-svc", 1)) {
                writer.appendBatch(b1, Map.of());
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, "gateway", 2)) {
                writer.appendBatch(b2, Map.of());
            }
            NormalizedSnapshot beforeCompaction = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(2, beforeCompaction.getFiles().size());
            path1 = beforeCompaction.getFiles().get(0).getPath();
            path2 = beforeCompaction.getFiles().get(1).getPath();

            // Compaction replaces both source files with one new merged
            // file -- path1/path2 stop being referenced by the CURRENT
            // snapshot, but remain referenced by now-historical ones
            // (Iceberg's time-travel), exactly the state expireSnapshots
            // is meant to clean up.
            new IcebergCompactor().compact(TableLocation.forLocal(tableRoot), Map.of(), allocator);

            // A real, confirmed-the-hard-way Iceberg subtlety, not assumed:
            // a removed data file only becomes eligible for physical
            // deletion once the snapshot that RECORDED its removal (the
            // compaction above) itself expires -- not when the snapshot
            // that originally added it expires. If the compaction's own
            // snapshot were still the retained "current" one, path1/path2
            // would never be cleaned up no matter how old they are. One
            // more commit after compaction is what makes the compaction
            // snapshot itself eligible for expiry under retainLast(1).
            try (VectorSchemaRoot b3 = buildBatch(allocator, "worker", 3)) {
                writer.appendBatch(b3, Map.of());
            }

            NormalizedSnapshot afterCompaction = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(2, afterCompaction.getFiles().size(), "the merged file plus the new append");

            assertTrue(Files.exists(Path.of(path1)), "source file must still physically exist before vacuum (compaction never deletes bytes)");
            assertTrue(Files.exists(Path.of(path2)), "source file must still physically exist before vacuum (compaction never deletes bytes)");

            // Negative retention -- deliberately expires every snapshot
            // older than "now + 1s" (i.e. everything that exists right
            // now), relying on retainLast(1) as the actual safety net
            // that decides what survives, rather than waiting on a real
            // multi-day wall-clock gap the way production retention
            // periods assume.
            List<String> deleted = new IcebergVacuum().vacuum(TableLocation.forLocal(tableRoot), Duration.ofSeconds(-1));

            assertTrue(deleted.contains(path1), "the orphaned source file must be deleted: " + deleted);
            assertTrue(deleted.contains(path2), "the orphaned source file must be deleted: " + deleted);
            assertFalse(Files.exists(Path.of(path1)), "orphaned source file must be physically gone");
            assertFalse(Files.exists(Path.of(path2)), "orphaned source file must be physically gone");

            NormalizedSnapshot afterVacuum = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(2, afterVacuum.getFiles().size(), "the live table itself is unaffected by vacuum");
            for (var file : afterVacuum.getFiles()) {
                assertTrue(Files.exists(Path.of(file.getPath())),
                        "every file the current snapshot actually needs must survive, regardless of the aggressive retention window: " + file.getPath());
            }
        }
    }

    @Test
    void vacuum_withinRetention_deletesNothing(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot batch = buildBatch(allocator, "auth-svc", 1)) {
                new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(batch, Map.of());
            }

            List<String> deleted = new IcebergVacuum().vacuum(TableLocation.forLocal(tableRoot), IcebergVacuum.DEFAULT_RETENTION);
            assertTrue(deleted.isEmpty(), "nothing is old enough to expire within the real default retention window");

            NormalizedSnapshot after = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(1, after.getFiles().size());
        }
    }
}
