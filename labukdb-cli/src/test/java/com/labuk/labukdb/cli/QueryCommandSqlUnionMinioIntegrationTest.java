package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Closes the gap {@link QueryCommandSqlUnionIntegrationTest} leaves open:
 * that test proves UNION/UNION ALL correctness against local fixtures only.
 * This is the first {@code --sql} UNION run against a real MinIO cluster,
 * not an inferred-safe composition of "UNION works locally" + "single-table
 * reads work against MinIO" -- same opt-in {@code MINIO_*} env-var gate as
 * every other MinIO integration test in this project.
 */
class QueryCommandSqlUnionMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;
    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void checkPreconditionsAndConnect(@TempDir java.nio.file.Path fakeHome) throws Exception {
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set -- skipping live MinIO union test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set -- skipping live MinIO union test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set -- skipping live MinIO union test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-union-minio-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        System.setOut(originalOut);
        System.setProperty("user.home", originalUserHome);
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    /** service(string), amount(long) -- a plain two-column Delta table on real MinIO, one commit. */
    private void buildTable(String uri, String[] services, long[] amounts) throws Exception {
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(service, amount));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < services.length; i++) {
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(services.length);
            CommitWriter.forLocationSingleWriterOnly(location).appendBatch(batch, Map.of());
        }
    }

    @Test
    void unionAll_realMinioBackedDeltaTables_keepsEveryRowFromBothSides() throws Exception {
        String errorsUri = testRootUri + "/errors";
        String infosUri = testRootUri + "/infos";
        buildTable(errorsUri, new String[] {"auth-svc"}, new long[] {1});
        buildTable(infosUri, new String[] {"gateway-svc", "auth-svc"}, new long[] {2, 1});

        TableCatalog.atDefaultLocation().register("errors", errorsUri);
        TableCatalog.atDefaultLocation().register("infos", infosUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors UNION ALL SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("gateway-svc | 2"), output);
        assertEquals(2, output.split("auth-svc \\| 1", -1).length - 1,
                "UNION ALL against real MinIO must keep the duplicate row: " + output);
        assertTrue(output.contains("3 row(s)"), output);
    }

    @Test
    void plainUnion_realMinioBackedDeltaTables_removesDuplicateRow() throws Exception {
        String errorsUri = testRootUri + "/errors";
        String infosUri = testRootUri + "/infos";
        buildTable(errorsUri, new String[] {"auth-svc"}, new long[] {1});
        buildTable(infosUri, new String[] {"gateway-svc", "auth-svc"}, new long[] {2, 1});

        TableCatalog.atDefaultLocation().register("errors", errorsUri);
        TableCatalog.atDefaultLocation().register("infos", infosUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors UNION SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertEquals(1, output.split("auth-svc \\| 1", -1).length - 1,
                "plain UNION against real MinIO must remove the duplicate row: " + output);
        assertTrue(output.contains("gateway-svc | 2"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }
}
