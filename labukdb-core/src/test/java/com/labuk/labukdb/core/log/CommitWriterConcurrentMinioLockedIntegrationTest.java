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
import java.util.Optional;
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
 * The actual end-to-end proof the {@link LockProvider} fix works: the exact
 * same 8-concurrent-writer-against-real-MinIO scenario as {@link
 * CommitWriterConcurrentMinioIntegrationTest} (which fails on purpose,
 * documenting the confirmed gap — left untouched, not modified or deleted),
 * but via {@link CommitWriter#forLocation(TableLocation, LockProvider)} with
 * a real {@link DynamoDbLockProvider} engaged. If this passes where the
 * unlocked version fails, that is the proof: the lock, not luck, is what
 * makes concurrent remote writers safe on storage whose own conditional-
 * create can't be trusted.
 *
 * Opt-in by design, gated on BOTH the existing {@code MINIO_*} env vars and
 * {@code DYNAMODB_LOCK_TABLE}/{@code DYNAMODB_ENDPOINT} (set the latter to
 * a running {@code amazon/dynamodb-local} container, or omit the endpoint
 * to use real AWS DynamoDB).
 */
class CommitWriterConcurrentMinioLockedIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping locked concurrent-writer test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping locked concurrent-writer test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping locked concurrent-writer test");

        Optional<DynamoDbLockConfiguration> lockConfig = DynamoDbLockConfiguration.fromEnvironment();
        assumeTrue(lockConfig.isPresent(), "DYNAMODB_LOCK_TABLE not set — skipping locked concurrent-writer test");
        lockProvider = DynamoDbLockProvider.fromConfiguration(lockConfig.get());

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-commitwriter-locked-concurrent-test/" + UUID.randomUUID();
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
    void appendBatch_concurrentWritersAgainstRealMinio_withLockEngaged_allCommitsLandWithNoLostUpdates() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(service, amount));
        new TableCreator().createTable(location, schema, List.of(), null);

        int writerCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writerCount);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>();

        for (int i = 0; i < writerCount; i++) {
            int id = i;
            futures.add(pool.submit(() -> {
                startGate.await();
                CommitWriter writer = CommitWriter.forLocation(location, lockProvider);
                try (VectorSchemaRoot batch = buildBatch(allocator, "writer-" + id, id)) {
                    return writer.appendBatch(batch, Map.of());
                }
            }));
        }

        startGate.countDown();
        Set<Long> versions = new HashSet<>();
        for (Future<Long> future : futures) {
            versions.add(future.get(120, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertEquals(writerCount, versions.size(),
                "with the lock engaged, every writer must land under a distinct version -- none lost, none double-assigned");

        Snapshot finalSnapshot = SnapshotManager.forLocation(location).latest();
        assertEquals(writerCount, finalSnapshot.getVersion());
        assertEquals(writerCount, finalSnapshot.getActiveFileCount());

        Set<String> expectedServices = new HashSet<>();
        for (int i = 0; i < writerCount; i++) {
            expectedServices.add("writer-" + i);
        }
        Set<String> actualServices = new HashSet<>();
        for (AddFile file : finalSnapshot.getActiveFiles()) {
            TableLocation dataFile = location.resolve(file.getPath());
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("service"), null, Map.of())) {
                actualServices.add(new String(
                        ((VarCharVector) readBack.getVector("service")).get(0), StandardCharsets.UTF_8));
            }
        }
        assertEquals(expectedServices, actualServices,
                "every writer's row is present and correct against real MinIO, with the lock engaged -- the actual fix proof");
    }
}
