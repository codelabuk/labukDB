package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.storage.DynamoDbLockConfiguration;
import com.labuk.labukdb.core.storage.MinioConfiguration;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The actual fix proof for {@link TableMergerConcurrentMinioIntegrationTest}'s
 * confirmed finding (7 of 8 concurrent inserts silently lost, zero
 * exceptions) — the exact same scenario, but through the new {@code
 * TableMerger.merge(TableLocation, LockProvider, ...)} overload this
 * session added, closing the gap where MERGE had no way to opt into
 * the DynamoDB-backed mitigation {@code appendBatch} already had.
 *
 * Opt-in, gated on both {@code MINIO_*} and {@code DYNAMODB_LOCK_TABLE}/
 * connection env vars, same convention as {@code
 * CommitWriterConcurrentMinioLockedIntegrationTest}.
 */
class TableMergerConcurrentMinioLockedIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;
    private LockProvider lockProvider;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping locked merge-concurrency test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping locked merge-concurrency test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping locked merge-concurrency test");

        var lockConfig = DynamoDbLockConfiguration.fromEnvironment();
        assumeTrue(lockConfig.isPresent(), "DYNAMODB_LOCK_TABLE not set — skipping locked merge-concurrency test");
        lockProvider = DynamoDbLockProvider.fromConfiguration(lockConfig.get());

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-tablemerger-locked-concurrent-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, long id, String service) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(idField, serviceField)), allocator);
        root.allocateNew();
        ((BigIntVector) root.getVector("id")).setSafe(0, id);
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
        return root;
    }

    @Test
    void merge_concurrentWritersAgainstRealMinio_withLockEngaged_allInsertsLand() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        new TableCreator().createTable(location, new Schema(List.of(idField, serviceField)), List.of(), null);

        int writerCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writerCount);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < writerCount; i++) {
            int id = i;
            futures.add(pool.submit(() -> {
                startGate.await();
                try (VectorSchemaRoot inserts = buildBatch(allocator, id, "writer-" + id)) {
                    new TableMerger().merge(location, lockProvider, Map.of(), List.of("id"), inserts, null, null, allocator);
                }
                return null;
            }));
        }

        startGate.countDown();
        for (Future<Void> future : futures) {
            future.get(120, TimeUnit.SECONDS); // must not throw -- the lock must let every writer eventually land
        }
        pool.shutdown();

        Snapshot finalSnapshot = SnapshotManager.forLocation(location).latest();
        Set<Long> actualIds = new HashSet<>();
        for (AddFile file : finalSnapshot.getActiveFiles()) {
            TableLocation dataFile = location.resolve(file.getPath());
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("id"), null, Map.of())) {
                for (int row = 0; row < readBack.getRowCount(); row++) {
                    actualIds.add(((BigIntVector) readBack.getVector("id")).get(row));
                }
            }
        }

        Set<Long> expectedIds = new HashSet<>();
        for (int i = 0; i < writerCount; i++) {
            expectedIds.add((long) i);
        }

        assertEquals(expectedIds, actualIds,
                "with the lock engaged, every writer's insert must be genuinely present -- "
                        + "the real fix proof for MERGE's own previously-unprotected commit");
    }
}
