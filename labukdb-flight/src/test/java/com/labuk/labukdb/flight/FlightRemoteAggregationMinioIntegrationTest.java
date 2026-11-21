package com.labuk.labukdb.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.CommitWriter;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves {@code GROUP BY}/{@code ORDER BY}/{@code LIMIT} over {@code DoGet}
 * (see {@link FlightAggregationIntegrationTest} for the local-disk version)
 * also works against a real remote (MinIO) table, not just locally — the
 * combine-mode code path (`CombinedScanTicket`, `combinedFlightInfo`)
 * resolves every file through {@link TableLocation} exactly like the
 * already-proven-remote plain-scan path, but hadn't been live-verified
 * before this test.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class FlightRemoteAggregationMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO aggregation test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO aggregation test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO aggregation test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-flight-remote-aggregation-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);

        allocator = new RootAllocator(Long.MAX_VALUE);
        buildTwoPartitionFixture();

        TableCatalog catalog = new TableCatalog(catalogDir.resolve("catalog.json"));
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

    /** ERROR: amounts 1..5 (sum 15, avg 3). INFO: amounts 100..104 (sum 510, avg 102). Same shape as the local fixture. */
    private void buildTwoPartitionFixture() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(service, amount));

        new TableCreator().createTable(location, schema, List.of("level"), null);

        writePartition(location, schema, "ERROR", 1, 5);
        writePartition(location, schema, "INFO", 100, 5);
    }

    private void writePartition(TableLocation location, Schema schema, String level, int start, int count) throws Exception {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < count; i++) {
                serviceVector.setSafe(i, (level.toLowerCase() + "-svc").getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, (long) (start + i));
            }
            batch.setRowCount(count);
            CommitWriter.forLocationSingleWriterOnly(location).appendBatch(batch, Map.of("level", level));
        }
    }

    @Test
    void groupBy_realMinioBackedTable_acrossMultipleFiles_combinesCorrectly() throws Exception {
        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(testRootUri) + ","
                + "\"groupBy\":[\"level\"],"
                + "\"aggregates\":["
                + "  {\"function\":\"COUNT\",\"column\":\"amount\",\"outputName\":\"cnt\"},"
                + "  {\"function\":\"SUM\",\"column\":\"amount\",\"outputName\":\"total\"},"
                + "  {\"function\":\"AVG\",\"column\":\"amount\",\"outputName\":\"avg\"},"
                + "  {\"function\":\"MIN\",\"column\":\"amount\",\"outputName\":\"min_amt\"},"
                + "  {\"function\":\"MAX\",\"column\":\"amount\",\"outputName\":\"max_amt\"}"
                + "]}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size(), "combine mode against a remote table must still register exactly one endpoint");

        Map<String, long[]> byLevel = new HashMap<>();
        Map<String, Double> avgByLevel = new HashMap<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    VarCharVector levelVector = (VarCharVector) root.getVector("level");
                    BigIntVector cnt = (BigIntVector) root.getVector("cnt");
                    Float8Vector total = (Float8Vector) root.getVector("total");
                    Float8Vector avg = (Float8Vector) root.getVector("avg");
                    BigIntVector minAmt = (BigIntVector) root.getVector("min_amt");
                    BigIntVector maxAmt = (BigIntVector) root.getVector("max_amt");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        String level = new String(levelVector.get(i), StandardCharsets.UTF_8);
                        byLevel.put(level, new long[] {cnt.get(i), (long) total.get(i), minAmt.get(i), maxAmt.get(i)});
                        avgByLevel.put(level, avg.get(i));
                    }
                }
            }
        }

        assertEquals(2, byLevel.size());
        assertArrayEquals(new long[] {5, 15, 1, 5}, byLevel.get("ERROR"));
        assertArrayEquals(new long[] {5, 510, 100, 104}, byLevel.get("INFO"));
        assertEquals(3.0, avgByLevel.get("ERROR"), 0.001);
        assertEquals(102.0, avgByLevel.get("INFO"), 0.001);
    }

    @Test
    void orderByLimit_realMinioBackedTable_noGroupBy_returnsGlobalTopN() throws Exception {
        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(testRootUri) + ","
                + "\"columns\":[\"amount\"],"
                + "\"orderBy\":[{\"column\":\"amount\",\"ascending\":false}],"
                + "\"limit\":3}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        List<Long> amounts = new java.util.ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    BigIntVector amountVector = (BigIntVector) root.getVector("amount");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        amounts.add(amountVector.get(i));
                    }
                }
            }
        }
        assertEquals(List.of(104L, 103L, 102L), amounts);
    }
}
