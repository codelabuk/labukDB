package com.labuk.labukdb.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.TableCreator;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.SyncPutListener;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The literal deliverable this pass exists for: a real {@code DoPut}
 * against a live MinIO-backed table, through the actual user-facing
 * Flight path — not just the library (see {@code RemoteWriteMinioIntegrationTest}
 * for that half). Same real-server/real-client shape
 * {@link FlightIntegrationTest} already uses for local tables.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class FlightRemoteWriteMinioIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightClient client;
    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;

    @BeforeEach
    void checkPreconditionsAndSetUp(@TempDir java.nio.file.Path catalogDir, @TempDir java.nio.file.Path walRoot) throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Flight write test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Flight write test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Flight write test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-flight-remote-write-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);

        // No Flight-level CREATE TABLE RPC yet (deliberately deferred) --
        // the table must already exist before DoPut, same as every local
        // DoPut test already assumes, just via the real TableCreator
        // instead of a hand-written commit.
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        new TableCreator().createTable(TableLocation.forRemote(testRootUri, conf),
                new Schema(List.of(service, amount)), List.of("level"), null);

        allocator = new RootAllocator(Long.MAX_VALUE);
        TableCatalog catalog = new TableCatalog(catalogDir.resolve("catalog.json"));
        // Explicit walRoot -- same reasoning as FlightIntegrationTest:
        // otherwise this would accumulate stale directories under the
        // real ~/.labukdb/wal on every test run.
        LabukDbFlightProducer producer = new LabukDbFlightProducer(allocator, catalog, conf, walRoot);

        Location serverLocation = Location.forGrpcInsecure("localhost", 0);
        server = FlightServer.builder(allocator, serverLocation, producer).build();
        server.start();

        Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
        client = FlightClient.builder(allocator, clientLocation).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
        if (allocator != null) allocator.close();
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
    }

    @Test
    void doPut_realMinioBackedTable_thenDoGet_roundTripsCorrectly() throws Exception {
        String writeJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(testRootUri) + ","
                + "\"partitionValues\":{\"level\":\"ERROR\"}}";
        FlightDescriptor putDescriptor = FlightDescriptor.command(writeJson.getBytes(StandardCharsets.UTF_8));

        try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "payments-svc"}, new long[] {1, 2});
             SyncPutListener putListener = new SyncPutListener()) {
            var putStream = client.startPut(putDescriptor, batch, putListener);
            putStream.putNext();
            putStream.completed();
            PutResult ack = putListener.read();
            assertNotNull(ack, "server must ack the put");
        }

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(testRootUri) + "}";
        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size(), "exactly one file was committed by the put");

        int totalRows = 0;
        java.util.List<String> services = new java.util.ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    totalRows += root.getRowCount();
                    VarCharVector serviceVector = (VarCharVector) root.getVector("service");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        services.add(new String(serviceVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        assertEquals(2, totalRows, "both rows from the DoPut were committed to real MinIO and are readable back");
        assertEquals(List.of("auth-svc", "payments-svc"), services);
    }

    private VectorSchemaRoot buildBatch(BufferAllocator alloc, String[] services, long[] amounts) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), alloc);
        root.allocateNew();
        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        BigIntVector amountVector = (BigIntVector) root.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, amounts[i]);
        }
        root.setRowCount(services.length);
        return root;
    }
}
