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
 * Settles, with real evidence rather than an assumption carried over
 * from {@code CommitWriterConcurrentMinioIntegrationTest}, whether
 * {@link TableMerger}'s own {@code replaceFiles} commit is exposed to
 * the identical confirmed-broken no-lock race on real MinIO — found by
 * direct inspection to be a real, previously-missing gap: {@code
 * TableMerger.merge} had no way to pass a {@link LockProvider} at all
 * until this session, unlike plain {@code appendBatch}. Threads, not
 * separate processes — proven sufficient for this exact race shape by
 * {@code CommitWriterConcurrentMinioIntegrationTest} already (the race
 * is at the remote storage layer, not an in-JVM static cache the way
 * Iceberg's default {@code LockManager} is).
 *
 * Opt-in, same {@code MINIO_*} env-var gate as every other MinIO
 * integration test in this project.
 */
class TableMergerConcurrentMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO merge-concurrency test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO merge-concurrency test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO merge-concurrency test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-tablemerger-concurrent-test/" + UUID.randomUUID();
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

    /**
     * The real question, matching {@code CommitWriterConcurrentMinioIntegrationTest}'s
     * own framing: does the number of genuinely visible, readable rows
     * match the number of writers that inserted a distinct key into the
     * same partition, or does the no-lock race silently drop some of
     * them? Each writer's own {@code TableMerger} instance, each
     * inserting a different {@code id} — any lost insert is a real,
     * silent data-loss bug, not a benign retry.
     */
    @Test
    void merge_concurrentWritersAgainstRealMinio_noLock_revealsActualBehavior() throws Exception {
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
                    new TableMerger().merge(location, Map.of(), List.of("id"), inserts, null, null, allocator);
                }
                return null;
            }));
        }

        startGate.countDown();
        int exceptions = 0;
        for (Future<Void> future : futures) {
            try {
                future.get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                exceptions++;
            }
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

        System.out.println("=== TABLEMERGER NO-LOCK MINIO RESULT ===");
        System.out.println("exceptions=" + exceptions + " / " + writerCount);
        System.out.println("expectedIds=" + expectedIds);
        System.out.println("actualIds=" + actualIds);

        // Intentionally not asserted as "must equal 8" -- unlike
        // CommitWriter's own test, this is the real discovery pass for
        // MERGE specifically; whichever outcome real MinIO actually
        // produces (clean exceptions after exhausting retries, or
        // silent data loss the way plain appendBatch showed) is what
        // this test exists to surface honestly, not assume in advance.
        assertEquals(expectedIds, actualIds,
                "every writer's insert must be genuinely present -- a mismatch here is the real, "
                        + "previously-unverified consequence of MERGE's own commit having had no lock option at all until now");
    }
}
