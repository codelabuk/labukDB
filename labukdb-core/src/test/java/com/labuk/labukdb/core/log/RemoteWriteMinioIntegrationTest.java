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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves remote WRITES actually work against a real cluster — the
 * counterpart to {@link RemoteReadMinioIntegrationTest} (reads) and
 * {@link HadoopLogStorageMinioIntegrationTest} (the raw atomic-commit
 * primitive in isolation). Before this ran, {@link TableCreator} and
 * {@link CommitWriter#forLocation} were remote-capable only by
 * inspection — this exercises the real round trip: create a brand-new
 * table on MinIO, append real data to it, read it back via the
 * already-verified remote read path.
 *
 * Opt-in by design, same shape as the other MinIO integration tests —
 * reads {@code MINIO_ENDPOINT}/{@code MINIO_ACCESS_KEY}/{@code MINIO_SECRET_KEY}
 * from the environment only, skips (not fails) when unset.
 */
class RemoteWriteMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO write test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO write test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO write test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-remote-write-test/" + UUID.randomUUID();
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

    @Test
    void createTable_thenAppendBatch_realRoundTripAgainstMinio() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);

        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(service, amount));

        long createdVersion = new TableCreator().createTable(location, schema, List.of("level"), null);
        assertEquals(0, createdVersion);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            ((VarCharVector) batch.getVector("service")).setSafe(0, "auth-svc".getBytes(StandardCharsets.UTF_8));
            ((BigIntVector) batch.getVector("amount")).setSafe(0, 99L);
            batch.setRowCount(1);

            long version = CommitWriter.forLocation(location).appendBatch(batch, Map.of("level", "ERROR"));
            assertEquals(1, version);
        }

        // Read back via the already-verified remote read path.
        Snapshot snapshot = SnapshotManager.forLocation(location).latest();
        assertEquals(1, snapshot.getVersion());
        assertEquals(1, snapshot.getActiveFileCount());
        AddFile file = snapshot.getActiveFiles().iterator().next();
        assertEquals(Map.of("level", "ERROR"), file.getPartitionValues());

        TableLocation dataFile = location.resolve(file.getPath());
        try (VectorSchemaRoot result = new ParquetToArrowReader().read(
                dataFile, allocator, List.of("service", "amount"), null, file.getPartitionValues())) {
            assertEquals(1, result.getRowCount());
            assertEquals("auth-svc", new String(
                    ((VarCharVector) result.getVector("service")).get(0), StandardCharsets.UTF_8));
            assertEquals(99L, ((BigIntVector) result.getVector("amount")).get(0));
        }
    }
}
