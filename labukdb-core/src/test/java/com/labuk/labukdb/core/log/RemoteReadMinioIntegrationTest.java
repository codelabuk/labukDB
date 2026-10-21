package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.planner.ScanPlan;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves remote (MinIO/S3-compatible) READS actually work against a real
 * cluster — the counterpart to {@link HadoopLogStorageMinioIntegrationTest},
 * which proved the same for writes. Before this ran, {@link TableLocation},
 * {@link SnapshotManager#forLocation}, and
 * {@link ParquetToArrowReader#read(TableLocation, BufferAllocator, List, Predicate, Map)}
 * were remote-capable only by inspection — never exercised against a real
 * object store. This closes that specific gap (local reads have been
 * real-table-verified repeatedly all session; remote reads never had been).
 *
 * <b>Opt-in by design</b>, same shape as {@link HadoopLogStorageMinioIntegrationTest}:
 * reads {@code MINIO_ENDPOINT}/{@code MINIO_ACCESS_KEY}/{@code MINIO_SECRET_KEY}
 * from the environment only, skips (not fails) when unset, deletes every
 * object it creates in {@code @AfterEach} regardless of outcome.
 *
 * <b>Table creation note:</b> there is still no CREATE TABLE support
 * anywhere in labukDB (a named, separate gap) — this test hand-writes a
 * minimal real commit log directly to MinIO, the same honest workaround
 * {@code CommitWriterTest}/{@code FlightIntegrationTest} already use for
 * local tables.
 */
class RemoteReadMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;

    @BeforeEach
    void checkPreconditionsAndConnect(@TempDir java.nio.file.Path localTemp) throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO read test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO read test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO read test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-remote-read-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);

        buildFixtureTable(localTemp);
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

    /**
     * One real Delta table on MinIO: one Parquet file (service, amount) under
     * {@code level=ERROR/}, committed as version 1 on top of a hand-written
     * version-0 metadata-only commit — same JSON shape
     * {@code CommitWriterTest.writeInitialCommit} and
     * {@code FlightIntegrationTest.writeCommitLog} already use for local
     * tables, written here with the real Hadoop {@code FileSystem} instead.
     */
    private void buildFixtureTable(java.nio.file.Path localTemp) throws Exception {
        VectorSchemaRoot batch = buildBatch();
        File localParquet = localTemp.resolve("part-0.snappy.parquet").toFile();
        try {
            new ArrowToParquetWriter().write(localParquet, batch);
        } finally {
            batch.close();
        }

        Path remoteDataFile = new Path(testRoot, "level=ERROR/part-0.snappy.parquet");
        byte[] bytes = Files.readAllBytes(localParquet.toPath());
        try (FSDataOutputStream out = fileSystem.create(remoteDataFile, false)) {
            out.write(bytes);
        }

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
        String version0 = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"remote-read-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}");
        writeCommitFile(0, version0);

        String version1 = "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\","
                + "\"partitionValues\":{\"level\":\"ERROR\"},\"size\":" + bytes.length + ","
                + "\"modificationTime\":1732000000500,\"dataChange\":true}}";
        writeCommitFile(1, version1);
    }

    private void writeCommitFile(long version, String content) throws Exception {
        Path commitFile = new Path(testRoot, "_delta_log/" + CommitFileNames.jsonFileName(version));
        try (FSDataOutputStream out = fileSystem.create(commitFile, false)) {
            out.write((content + "\n").getBytes(StandardCharsets.UTF_8));
        }
    }

    private VectorSchemaRoot buildBatch() {
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(service, amount)), allocator);
        root.allocateNew();
        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        BigIntVector amountVector = (BigIntVector) root.getVector("amount");
        String[] services = {"auth-svc", "payments-svc"};
        long[] amounts = {10, 20};
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, amounts[i]);
        }
        root.setRowCount(services.length);
        return root;
    }

    @Test
    void snapshotPruningAndDecode_workCorrectlyAgainstRealMinio() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);

        Snapshot snapshot = SnapshotManager.forLocation(location).latest();
        assertEquals(1, snapshot.getVersion());
        assertEquals(1, snapshot.getActiveFileCount());
        AddFile file = snapshot.getActiveFiles().iterator().next();
        assertEquals(Map.of("level", "ERROR"), file.getPartitionValues());

        QueryPlanner planner = new QueryPlanner();
        ScanPlan matching = planner.plan(snapshot, Predicate.compare("level", ComparisonOp.EQ, "ERROR"), null);
        assertEquals(1, matching.getFilesToScan().size(), "level=ERROR must survive partition pruning on a remotely-resolved snapshot");

        ScanPlan nonMatching = planner.plan(snapshot, Predicate.compare("level", ComparisonOp.EQ, "INFO"), null);
        assertEquals(0, nonMatching.getFilesToScan().size(), "level=INFO must be pruned out — the file is only ever level=ERROR");

        TableLocation dataFile = location.resolve(file.getPath());
        try (VectorSchemaRoot result = new ParquetToArrowReader().read(
                dataFile, allocator, List.of("service", "amount", "level"), null, file.getPartitionValues())) {
            assertEquals(2, result.getRowCount());
            VarCharVector serviceVector = (VarCharVector) result.getVector("service");
            BigIntVector amountVector = (BigIntVector) result.getVector("amount");
            VarCharVector levelVector = (VarCharVector) result.getVector("level");
            assertEquals("auth-svc", new String(serviceVector.get(0), StandardCharsets.UTF_8));
            assertEquals("payments-svc", new String(serviceVector.get(1), StandardCharsets.UTF_8));
            assertEquals(10L, amountVector.get(0));
            assertEquals(20L, amountVector.get(1));
            assertEquals("ERROR", new String(levelVector.get(0), StandardCharsets.UTF_8));
        }
    }
}
