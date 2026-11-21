package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link OrphanFileCleaner}'s remote listing path ({@code
 * FileSystem.listFiles(path, recursive=true)}) is genuinely new code,
 * never exercised against a real backend before this -- unlike {@link
 * Vacuum}, which only ever deletes a path the log already names,
 * {@code OrphanFileCleaner} has to independently enumerate real files
 * that exist in MinIO, which is a different code path entirely (recursive
 * listing vs. targeted deletion). The actual payoff assertion is the same
 * standard {@code CompactorVacuumRemoteMinioIntegrationTest} already
 * uses: a real {@code FileSystem.exists()} check against the bucket, not
 * just a log entry.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class OrphanFileCleanerMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;
    private TableLocation location;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO orphan-cleanup test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO orphan-cleanup test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO orphan-cleanup test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-orphan-cleanup-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
        location = TableLocation.forRemote(testRootUri, conf);

        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        new TableCreator().createTable(location, new Schema(List.of(service)), List.of(), null);
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

    private VectorSchemaRoot buildBatch(String service) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField)), allocator);
        root.allocateNew();
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
        return root;
    }

    @Test
    void uncommittedFileOnRealMinio_cleanedUp_activeFileNeverTouched() throws Exception {
        try (VectorSchemaRoot committed = buildBatch("auth-svc")) {
            CommitWriter.forLocationSingleWriterOnly(location).appendBatch(committed, Map.of());
        }

        Snapshot snapshot = SnapshotManager.forLocation(location).latest();
        AddFile activeFile = snapshot.getActiveFiles().iterator().next();

        // Simulate a crashed writer: a real Parquet file written straight to
        // MinIO, with no matching commit at all.
        try (VectorSchemaRoot uncommitted = buildBatch("crashed-write")) {
            new ArrowToParquetWriter().write(location.resolve("crashed-writer.parquet"), uncommitted);
        }
        assertTrue(fileSystem.exists(location.resolve("crashed-writer.parquet").hadoopPath()),
                "sanity check: the orphan file genuinely exists on MinIO before cleanup runs");

        // Duration.ZERO: cutoff is "now" -- the files above were written
        // moments ago over a real network round trip, old enough to count
        // without a hand-set, synthetic modification time.
        List<String> deleted = new OrphanFileCleaner().clean(location, Duration.ZERO);

        assertEquals(List.of("crashed-writer.parquet"), deleted);
        assertFalse(fileSystem.exists(location.resolve("crashed-writer.parquet").hadoopPath()),
                "the orphan's bytes must be genuinely gone from MinIO");
        assertTrue(fileSystem.exists(location.resolve(activeFile.getPath()).hadoopPath()),
                "the real, committed file must never be touched");

        Snapshot after = SnapshotManager.forLocation(location).latest();
        assertEquals(1, after.getActiveFileCount(), "the table's own active file count is unaffected");
    }
}
