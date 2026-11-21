package com.labuk.labukdb.core.log;

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
 * Proves {@link Compactor} and {@link Vacuum} work against a real remote
 * (MinIO) table — both were genuinely local-only before this (local-only
 * {@code java.io.File} decode reads, and for {@code Vacuum}, plain
 * {@code java.nio.file.Files.deleteIfExists} for deletion, which cannot
 * target {@code s3a://} at all).
 *
 * <b>The actual payoff assertion</b> is a real {@code FileSystem.exists()}
 * check against the bucket after vacuuming — proving bytes were genuinely
 * deleted from MinIO, not just that a log entry says so.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class CompactorVacuumRemoteMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Compactor/Vacuum test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Compactor/Vacuum test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Compactor/Vacuum test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-compactor-vacuum-remote-test/" + UUID.randomUUID();
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
    void compactThenVacuum_realMinioBackedTable_bytesActuallyDeletedFromMinio() throws Exception {
        CommitWriter writer = CommitWriter.forLocationSingleWriterOnly(location);
        try (VectorSchemaRoot b1 = buildBatch("svc-1")) {
            writer.appendBatch(b1, Map.of());
        }
        try (VectorSchemaRoot b2 = buildBatch("svc-2")) {
            writer.appendBatch(b2, Map.of());
        }
        try (VectorSchemaRoot b3 = buildBatch("svc-3")) {
            writer.appendBatch(b3, Map.of());
        }

        Snapshot beforeCompact = SnapshotManager.forLocation(location).latest();
        assertEquals(3, beforeCompact.getActiveFileCount(), "three separate appends -> three separate files");
        List<AddFile> originalFiles = List.copyOf(beforeCompact.getActiveFiles());

        long compactedVersion = new Compactor().compact(location, Map.of(), allocator);
        assertTrue(compactedVersion > 0);

        Snapshot afterCompact = SnapshotManager.forLocation(location).latest();
        assertEquals(1, afterCompact.getActiveFileCount(), "three files compacted into one");

        // Compaction only changes the log -- the old files' bytes must
        // still physically exist on MinIO until Vacuum actually runs.
        for (AddFile file : originalFiles) {
            assertTrue(fileSystem.exists(location.resolve(file.getPath()).hadoopPath()),
                    "Compactor must not delete bytes -- that's Vacuum's job, deliberately separate: " + file.getPath());
        }

        // Duration.ZERO: the cutoff is "now", and Compactor's real remove
        // actions carry a real deletionTimestamp from a few moments ago
        // (real network round trip already elapsed) -- old enough to
        // count as stale without needing a hand-written, synthetic
        // deletionTimestamp.
        List<String> deleted = new Vacuum().vacuum(location, Duration.ZERO);
        assertEquals(3, deleted.size(), "all three original (now-removed) files are old enough to vacuum");

        for (AddFile file : originalFiles) {
            assertFalse(fileSystem.exists(location.resolve(file.getPath()).hadoopPath()),
                    "bytes must be genuinely gone from MinIO, not just logically removed: " + file.getPath());
        }

        // The still-active (compacted) file must survive and remain readable.
        AddFile activeFile = afterCompact.getActiveFiles().iterator().next();
        assertTrue(fileSystem.exists(location.resolve(activeFile.getPath()).hadoopPath()),
                "the currently-active compacted file must never be touched by vacuum");
    }
}
