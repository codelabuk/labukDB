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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Iceberg counterpart of {@code core.log.TableMergerTest} — same
 * {@code id}(long key)/{@code service}(string) schema, same test
 * shapes, proving {@link IcebergTableMerger} gives Delta's upsert/MERGE
 * semantics (via the shared {@code PartitionRowMerger}) on a real
 * Iceberg table, including the one case that actually proves the
 * freshness check matters: a concurrent append racing a merge in the
 * same partition.
 */
class IcebergTableMergerTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "service", Types.StringType.get()));

    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, long[] ids, String[] services) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(idField, serviceField)), allocator);
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("id");
        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i]);
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(ids.length);
        return root;
    }

    private Map<Long, String> readBack(String tableRoot, BufferAllocator allocator) throws Exception {
        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot, new Configuration()).latest();
        Map<Long, String> result = new HashMap<>();
        for (DataFileRef file : snapshot.getFiles()) {
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    new java.io.File(file.getPath()), allocator, List.of("id", "service"), null, Map.of())) {
                BigIntVector idVector = (BigIntVector) readBack.getVector("id");
                VarCharVector serviceVector = (VarCharVector) readBack.getVector("service");
                for (int i = 0; i < readBack.getRowCount(); i++) {
                    result.put(idVector.get(i), new String(serviceVector.get(i), StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    @Test
    void merge_insertOnly_intoBrandNewPartition_createsOneFile(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot inserts = buildBatch(allocator, new long[] {1, 2}, new String[] {"auth-svc", "payments-svc"})) {
            new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                    inserts, null, null, allocator);

            Map<Long, String> rows = readBack(tableRoot.toString(), allocator);
            assertEquals(Map.of(1L, "auth-svc", 2L, "payments-svc"), rows);

            NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(1, snapshot.getFiles().size());
        }
    }

    @Test
    void merge_updateByKey_replacesOldValue(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"old-name"})) {
                new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot updates = buildBatch(allocator, new long[] {1}, new String[] {"new-name"})) {
                new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                        null, updates, null, allocator);
            }
            Map<Long, String> rows = readBack(tableRoot.toString(), allocator);
            assertEquals(Map.of(1L, "new-name"), rows, "the old value must be gone, not just appended alongside");
        }
    }

    @Test
    void merge_deleteByKey_removesOnlyThatRow(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1, 2}, new String[] {"keep-me", "delete-me"})) {
                new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot deletes = buildBatch(allocator, new long[] {2}, new String[] {"ignored"})) {
                new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                        null, null, deletes, allocator);
            }
            Map<Long, String> rows = readBack(tableRoot.toString(), allocator);
            assertEquals(Map.of(1L, "keep-me"), rows);
        }
    }

    @Test
    void merge_deleteEveryRowInPartition_leavesNoActiveFiles(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"only-row"})) {
                new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot deletes = buildBatch(allocator, new long[] {1}, new String[] {"ignored"})) {
                new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                        null, null, deletes, allocator);
            }
            NormalizedSnapshot after = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(0, after.getFiles().size(), "deleting every row must leave zero active files");
        }
    }

    @Test
    void merge_mixedInsertUpdateDelete_acrossMultiplePreExistingFiles(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
            try (VectorSchemaRoot b1 = buildBatch(allocator, new long[] {1}, new String[] {"keep"})) {
                writer.appendBatch(b1, Map.of());
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new long[] {2}, new String[] {"update-me"})) {
                writer.appendBatch(b2, Map.of());
            }
            try (VectorSchemaRoot b3 = buildBatch(allocator, new long[] {3}, new String[] {"delete-me"})) {
                writer.appendBatch(b3, Map.of());
            }

            try (VectorSchemaRoot inserts = buildBatch(allocator, new long[] {4}, new String[] {"brand-new"});
                 VectorSchemaRoot updates = buildBatch(allocator, new long[] {2}, new String[] {"updated"});
                 VectorSchemaRoot deletes = buildBatch(allocator, new long[] {3}, new String[] {"ignored"})) {
                new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                        inserts, updates, deletes, allocator);
            }

            Map<Long, String> rows = readBack(tableRoot.toString(), allocator);
            assertEquals(Map.of(1L, "keep", 2L, "updated", 4L, "brand-new"), rows);

            NormalizedSnapshot after = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(1, after.getFiles().size(),
                    "the three original files plus the merge result must collapse into exactly one new file");
        }
    }

    @Test
    void merge_missingKeyColumn_throwsClearValidationError(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot inserts = buildBatch(allocator, new long[] {1}, new String[] {"x"})) {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("does_not_exist"),
                            inserts, null, null, allocator));
            assertTrue(thrown.getMessage().contains("does_not_exist"), "error must name the missing column: " + thrown.getMessage());
        }
    }

    @Test
    void merge_concurrentAppendToSamePartition_bothWritersLandNoDataLost(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"original"})) {
                new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(existing, Map.of());
            }

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch gate = new CountDownLatch(1);

            Future<?> mergeFuture = pool.submit(() -> {
                try {
                    gate.await();
                    try (VectorSchemaRoot inserts = buildBatch(allocator, new long[] {2}, new String[] {"from-merge"})) {
                        new IcebergTableMerger().merge(TableLocation.forLocal(tableRoot), Map.of(), List.of("id"),
                                inserts, null, null, allocator);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Future<?> appendFuture = pool.submit(() -> {
                try {
                    gate.await();
                    try (VectorSchemaRoot concurrent = buildBatch(allocator, new long[] {3}, new String[] {"from-concurrent-append"})) {
                        new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(concurrent, Map.of());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            gate.countDown();
            mergeFuture.get(30, TimeUnit.SECONDS);
            appendFuture.get(30, TimeUnit.SECONDS);
            pool.shutdown();

            Map<Long, String> rows = readBack(tableRoot.toString(), allocator);
            assertEquals(Map.of(1L, "original", 2L, "from-merge", 3L, "from-concurrent-append"), rows,
                    "a concurrent plain append racing a merge in the same partition must not lose either writer's row, "
                            + "and the merge's own freshness check must not silently drop the concurrently-added file");
        }
    }

    @Test
    void merge_catalogBased_updateByKey_replacesOldValue(@TempDir Path warehouse) throws Exception {
        InMemoryCatalog inMemory = new InMemoryCatalog();
        inMemory.initialize("test", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString()));
        Catalog catalog = inMemory;
        TableIdentifier id = TableIdentifier.of("analytics", "logs");
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("analytics"));
        catalog.createTable(id, ICEBERG_SCHEMA, UNPARTITIONED);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"old-name"})) {
                new IcebergTableWriter(catalog, id).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot updates = buildBatch(allocator, new long[] {1}, new String[] {"new-name"})) {
                new IcebergTableMerger().merge(catalog, id, Map.of(), List.of("id"), null, updates, null, allocator);
            }

            NormalizedSnapshot snapshot = new IcebergTableFormat(catalog, id).latest();
            Map<Long, String> rows = new HashMap<>();
            for (DataFileRef file : snapshot.getFiles()) {
                try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                        new java.io.File(file.getPath()), allocator, List.of("id", "service"), null, Map.of())) {
                    BigIntVector idVector = (BigIntVector) readBack.getVector("id");
                    VarCharVector serviceVector = (VarCharVector) readBack.getVector("service");
                    for (int i = 0; i < readBack.getRowCount(); i++) {
                        rows.put(idVector.get(i), new String(serviceVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
            assertEquals(Map.of(1L, "new-name"), rows);
        }
    }
}
