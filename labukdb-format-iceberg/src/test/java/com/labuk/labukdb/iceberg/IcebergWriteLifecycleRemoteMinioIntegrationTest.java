package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The one real coverage gap this closes: {@link IcebergTableCreator},
 * {@link IcebergTableMerger}, {@link IcebergCompactor}, and {@link
 * IcebergVacuum} were all built and tested only against local disk —
 * none had ever actually been run against a live remote cluster. A
 * single end-to-end lifecycle (create → append → merge → compact →
 * vacuum), each step against real MinIO, is the real proof these four
 * classes' {@code TableLocation.isRemote()} dispatch (already proven
 * correct for {@link IcebergTableWriter} by {@code
 * IcebergWriterRemoteMinioIntegrationTest}) holds for the rest of the
 * write-lifecycle surface too — not four separate round trips repeating
 * the same create-table setup, which would cost real wall-clock time
 * without adding real coverage.
 *
 * Same opt-in env-var gate, fixture-build, and cleanup pattern as every
 * other MinIO integration test in this project.
 */
class IcebergWriteLifecycleRemoteMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Iceberg write-lifecycle test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Iceberg write-lifecycle test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Iceberg write-lifecycle test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-iceberg-lifecycle-remote-test/" + UUID.randomUUID();
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

    private Map<Long, String> readBackAll(BufferAllocator allocator) throws Exception {
        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();
        Map<Long, String> result = new HashMap<>();
        for (DataFileRef file : snapshot.getFiles()) {
            TableLocation dataFile = TableLocation.forRemote(file.getPath(), conf);
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("id", "service"), null, Map.of())) {
                for (int row = 0; row < readBack.getRowCount(); row++) {
                    result.put(((BigIntVector) readBack.getVector("id")).get(row),
                            new String(((VarCharVector) readBack.getVector("service")).get(row), StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    @Test
    void fullWriteLifecycle_createAppendMergeCompactVacuum_allAgainstRealMinio() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);

        // 1. CREATE TABLE
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema arrowSchema = new Schema(List.of(idField, serviceField));
        new IcebergTableCreator().createTable(location, arrowSchema, List.of(), null);

        NormalizedSnapshot afterCreate = new IcebergTableFormat(testRootUri, conf).latest();
        assertEquals(0, afterCreate.getFiles().size(), "a freshly created table has no data files yet");

        // 2. APPEND (x2, separate files -- gives compaction something real to do later)
        IcebergTableWriter writer = new IcebergTableWriter(location);
        try (VectorSchemaRoot b1 = buildBatch(allocator, 1, "auth-svc")) {
            writer.appendBatch(b1, Map.of());
        }
        try (VectorSchemaRoot b2 = buildBatch(allocator, 2, "old-gateway")) {
            writer.appendBatch(b2, Map.of());
        }
        assertEquals(2, new IcebergTableFormat(testRootUri, conf).latest().getFiles().size());

        // 3. MERGE -- update id=2, insert id=3
        try (VectorSchemaRoot updates = buildBatch(allocator, 2, "gateway");
             VectorSchemaRoot inserts = buildBatch(allocator, 3, "payments-svc")) {
            new IcebergTableMerger().merge(location, Map.of(), List.of("id"), inserts, updates, null, allocator);
        }
        assertEquals(Map.of(1L, "auth-svc", 2L, "gateway", 3L, "payments-svc"), readBackAll(allocator),
                "merge against real MinIO must apply the update and the insert correctly");

        // MERGE's own OverwriteFiles commit already collapsed the two
        // append files into one merged file -- an extra append here is
        // what actually gives compaction a genuine 2-files case,
        // otherwise it would correctly no-op (fewer than 2 active files).
        try (VectorSchemaRoot b3b = buildBatch(allocator, 5, "extra-before-compaction")) {
            writer.appendBatch(b3b, Map.of());
        }
        assertEquals(2, new IcebergTableFormat(testRootUri, conf).latest().getFiles().size());

        // 4. COMPACT -- collapse those 2 files into one
        long filesBeforeCompaction = new IcebergTableFormat(testRootUri, conf).latest().getFiles().size();
        long compactedSnapshotId = new IcebergCompactor().compact(location, Map.of(), allocator);
        assertNotEquals(-1, compactedSnapshotId, "there must be at least 2 files to compact: " + filesBeforeCompaction);
        NormalizedSnapshot afterCompaction = new IcebergTableFormat(testRootUri, conf).latest();
        assertEquals(1, afterCompaction.getFiles().size(), "compaction against real MinIO must collapse to one file");
        assertEquals(Map.of(1L, "auth-svc", 2L, "gateway", 3L, "payments-svc", 5L, "extra-before-compaction"), readBackAll(allocator),
                "compaction must not change the logical data, only which file holds it");

        // One more commit, so the compaction snapshot itself isn't the
        // "current" one -- required for vacuum to actually reclaim the
        // files it orphaned (see IcebergVacuum's own javadoc for why).
        try (VectorSchemaRoot b4 = buildBatch(allocator, 4, "worker")) {
            writer.appendBatch(b4, Map.of());
        }

        // 5. VACUUM -- aggressive retention (same technique IcebergVacuumTest
        // uses locally): expire everything except the current snapshot,
        // relying on retainLast(1) as the real safety net.
        List<String> deleted = new IcebergVacuum().vacuum(location, Duration.ofSeconds(-1));
        System.out.println("=== ICEBERG LIFECYCLE MINIO VACUUM ===");
        System.out.println("deleted=" + deleted);

        NormalizedSnapshot afterVacuum = new IcebergTableFormat(testRootUri, conf).latest();
        assertEquals(2, afterVacuum.getFiles().size(), "the compacted file plus the final append must both survive");
        assertEquals(Map.of(1L, "auth-svc", 2L, "gateway", 3L, "payments-svc", 4L, "worker", 5L, "extra-before-compaction"), readBackAll(allocator),
                "vacuum against real MinIO must leave the live table's actual data completely unaffected");
        for (DataFileRef file : afterVacuum.getFiles()) {
            assertTrue(fileSystem.exists(new Path(file.getPath())), "every file the current snapshot needs must still physically exist: " + file.getPath());
        }
    }
}
