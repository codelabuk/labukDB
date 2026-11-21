package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.action.AddFile;
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

import java.io.File;
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
 * The real-concurrency counterpart of {@link CommitWriterTest
 * #appendBatch_concurrentWriters_allCommitsLandWithNoLostUpdates}, redirected
 * at a live MinIO cluster — closing the gap the project's own docs admitted:
 * remote OCC conflict-retry had only ever been exercised by a single writer
 * ({@link HadoopLogStorageMinioIntegrationTest} proves one conflicting
 * {@code create} throws {@code FileAlreadyExistsException}, and {@link
 * RemoteWriteMinioIntegrationTest} proves one writer's round trip), never by
 * genuinely concurrent writers racing the same remote table.
 *
 * Same N-independent-{@code CommitWriter}-instances shape as the local
 * test (simulating N separate processes, not one shared instance, against
 * one shared table) — with plain create-if-absent semantics and no retry,
 * only one writer would ever land and the rest would throw; this proves the
 * optimistic-concurrency retry loop survives real contention over a real
 * network round trip to S3-compatible storage, not just local-disk
 * {@code Files.newByteChannel(CREATE_NEW)} contention.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration test.
 */
class CommitWriterConcurrentMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO concurrent-writer test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO concurrent-writer test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO concurrent-writer test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-commitwriter-concurrent-test/" + UUID.randomUUID();
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
    void appendBatch_concurrentWritersAgainstRealMinio_allCommitsLandWithNoLostUpdates() throws Exception {
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
                CommitWriter writer = CommitWriter.forLocationSingleWriterOnly(location);
                try (VectorSchemaRoot batch = buildBatch(allocator, "writer-" + id, id)) {
                    return writer.appendBatch(batch, Map.of());
                }
            }));
        }

        startGate.countDown(); // release all writers at once -- maximizes real collisions over the network
        Set<Long> versions = new HashSet<>();
        for (Future<Long> future : futures) {
            versions.add(future.get(120, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertEquals(writerCount, versions.size(),
                "every writer landed under a distinct version against real MinIO -- none lost, none double-assigned");

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
        assertEquals(expectedServices, actualServices, "every writer's row is present and correct -- no lost updates, real MinIO");
    }
}
