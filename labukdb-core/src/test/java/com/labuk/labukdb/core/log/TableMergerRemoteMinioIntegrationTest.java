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
 * Proves {@link TableMerger} (insert/update/delete by key) works against a
 * real remote (MinIO) table, not just locally — {@code TableMerger} was
 * genuinely local-only before this (hardcoded {@code Path}/local
 * {@code SnapshotManager}/{@code CommitWriter} construction throughout),
 * not just unverified.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class TableMergerRemoteMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO TableMerger test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO TableMerger test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO TableMerger test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-tablemerger-remote-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
        location = TableLocation.forRemote(testRootUri, conf);

        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        new TableCreator().createTable(location, new Schema(List.of(id, service)), List.of(), null);
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

    private VectorSchemaRoot buildBatch(long[] ids, String[] services) {
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

    private Map<Long, String> readBack() throws Exception {
        Snapshot snapshot = SnapshotManager.forLocation(location).latest();
        Map<Long, String> result = new java.util.HashMap<>();
        for (AddFile file : snapshot.getActiveFiles()) {
            TableLocation dataFile = location.resolve(file.getPath());
            try (VectorSchemaRoot batch = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("id", "service"), null, Map.of())) {
                BigIntVector idVector = (BigIntVector) batch.getVector("id");
                VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
                for (int i = 0; i < batch.getRowCount(); i++) {
                    result.put(idVector.get(i), new String(serviceVector.get(i), StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    @Test
    void merge_realMinioBackedTable_insertUpdateDelete() throws Exception {
        try (VectorSchemaRoot insert = buildBatch(new long[] {1, 2}, new String[] {"auth-svc", "payments-svc"})) {
            new TableMerger().merge(location, Map.of(), List.of("id"), insert, null, null, allocator);
        }
        assertEquals(Map.of(1L, "auth-svc", 2L, "payments-svc"), readBack());

        try (VectorSchemaRoot update = buildBatch(new long[] {1}, new String[] {"auth-svc-UPDATED"})) {
            new TableMerger().merge(location, Map.of(), List.of("id"), null, update, null, allocator);
        }
        assertEquals(Map.of(1L, "auth-svc-UPDATED", 2L, "payments-svc"), readBack());

        try (VectorSchemaRoot delete = buildBatch(new long[] {2}, new String[] {"ignored"})) {
            new TableMerger().merge(location, Map.of(), List.of("id"), null, null, delete, allocator);
        }
        assertEquals(Map.of(1L, "auth-svc-UPDATED"), readBack());
    }
}
