package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link IcebergTableWriter}'s concurrency story under real
 * contention, not just functionally. {@code iceberg-core}'s own
 * {@code SnapshotProducer.commit()} already runs a bounded,
 * exponential-backoff optimistic-concurrency retry internally (confirmed
 * via {@code javap -c} on real bytecode), but that confirmation was never
 * exercised by genuinely concurrent writers in this project before —
 * only single-writer, sequential {@code appendBatch} calls
 * ({@link IcebergTableWriterTest}).
 *
 * Same N-independent-writer-instances shape as {@code CommitWriter}'s own
 * local stress test ({@code CommitWriterTest
 * #appendBatch_concurrentWriters_allCommitsLandWithNoLostUpdates}):
 * N {@link IcebergTableWriter}s (simulating N separate processes) race
 * {@code appendBatch} against one shared, already-created, unpartitioned
 * table at once. If {@code iceberg-core}'s internal retry didn't actually
 * work, some writers would silently lose their commit to a racing one
 * instead of retrying against the new base snapshot.
 */
class IcebergTableWriterConcurrentTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()));

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
    void appendBatch_concurrentWriters_allCommitsLandWithNoLostUpdates(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), Map.of(), tableRoot.toString());

        int writerCount = 10;
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ExecutorService pool = Executors.newFixedThreadPool(writerCount);
            CountDownLatch startGate = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>();

            for (int i = 0; i < writerCount; i++) {
                int id = i;
                futures.add(pool.submit(() -> {
                    startGate.await();
                    IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
                    try (VectorSchemaRoot batch = buildBatch(allocator, "writer-" + id, id)) {
                        writer.appendBatch(batch, Map.of());
                    }
                    return null;
                }));
            }

            startGate.countDown(); // release all writers at once -- maximizes real collisions on the shared snapshot
            for (Future<Void> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(writerCount, snapshot.getFiles().size(),
                    "every concurrent writer's file must be committed -- none lost to a racing commit");

            Set<String> expectedServices = new HashSet<>();
            for (int i = 0; i < writerCount; i++) {
                expectedServices.add("writer-" + i);
            }
            Set<String> actualServices = new HashSet<>();
            for (DataFileRef file : snapshot.getFiles()) {
                try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                        new File(file.getPath()), allocator, List.of("service"), null, Map.of())) {
                    actualServices.add(new String(
                            ((VarCharVector) readBack.getVector("service")).get(0), StandardCharsets.UTF_8));
                }
            }
            assertEquals(expectedServices, actualServices, "every writer's row is present and correct -- no lost updates");
        }
    }
}
