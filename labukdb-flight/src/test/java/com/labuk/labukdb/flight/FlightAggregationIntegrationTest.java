package com.labuk.labukdb.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the whole point of this pass: a {@code GROUP BY}/{@code ORDER BY}/
 * {@code LIMIT} query spanning <b>multiple physical files</b> gets combined
 * correctly on the server, not answered per-file. The fixture deliberately
 * spreads rows across two partition files ({@code level=ERROR},
 * {@code level=INFO}) so a correct implementation has to actually combine
 * them — a per-file-only implementation would silently produce two partial
 * results (or a wrong top-N) instead of one correct one.
 */
class FlightAggregationIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String FILE_SCHEMA_STRING =
            "message test { required int64 amount; required binary message (UTF8); }";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightClient client;
    private TableCatalog catalog;
    // See FlightIntegrationTest's matching field: supplied explicitly so
    // tearDown can shut it down and await its drain itself, since
    // FlightServer.close() only awaitTermination()s the underlying
    // io.grpc.Server, never this executor -- the leading suspect for this
    // suite's intermittent "Memory was leaked by query" flake.
    private ExecutorService grpcExecutor;

    @TempDir
    Path catalogDir;

    @TempDir
    Path walRoot;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        catalog = new TableCatalog(catalogDir.resolve("catalog.json"));
        LabukDbFlightProducer producer = new LabukDbFlightProducer(allocator, catalog, new Configuration(), walRoot);

        grpcExecutor = Executors.newCachedThreadPool();
        Location serverLocation = Location.forGrpcInsecure("localhost", 0);
        server = FlightServer.builder(allocator, serverLocation, producer)
                .executor(grpcExecutor)
                .build();
        server.start();

        Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
        client = FlightClient.builder(allocator, clientLocation).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.close();
        grpcExecutor.shutdown();
        if (!grpcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            grpcExecutor.shutdownNow();
        }
        // Allocator-leak check, same pattern every Flight test already
        // uses: if the new decode -> aggregate -> sort pipeline's resource
        // cleanup is wrong, this throws IllegalStateException here.
        allocator.close();
    }

    private void writePartitionFile(Path partitionDir, String partitionValue, int startAmount, int count) throws Exception {
        Files.createDirectories(partitionDir);
        MessageType schema = MessageTypeParser.parseMessageType(FILE_SCHEMA_STRING);
        File file = partitionDir.resolve("part-0.snappy.parquet").toFile();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < count; i++) {
                long amount = startAmount + i;
                Group group = factory.newGroup()
                        .append("amount", amount)
                        .append("message", partitionValue.toLowerCase() + "-msg-" + amount);
                writer.write(group);
            }
        }
    }

    private void writeCommitLog(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        long errorFileSize = Files.size(tableRoot.resolve("level=ERROR/part-0.snappy.parquet"));
        long infoFileSize = Files.size(tableRoot.resolve("level=INFO/part-0.snappy.parquet"));

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"message\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"aggregation-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + MAPPER.writeValueAsString(schemaString) + ","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}",
                "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"ERROR\"},"
                        + "\"size\":" + errorFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"level=INFO/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"INFO\"},"
                        + "\"size\":" + infoFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    /** ERROR: amounts 1..5 (sum 15, avg 3). INFO: amounts 100..104 (sum 510, avg 102). */
    private void buildTwoPartitionFixture(Path tableRoot) throws Exception {
        writePartitionFile(tableRoot.resolve("level=ERROR"), "ERROR", 1, 5);
        writePartitionFile(tableRoot.resolve("level=INFO"), "INFO", 100, 5);
        writeCommitLog(tableRoot);
    }

    @Test
    void groupBy_acrossMultipleFiles_combinesCorrectly(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"groupBy\":[\"level\"],"
                + "\"aggregates\":["
                + "  {\"function\":\"COUNT\",\"column\":\"amount\",\"outputName\":\"cnt\"},"
                + "  {\"function\":\"SUM\",\"column\":\"amount\",\"outputName\":\"total\"},"
                + "  {\"function\":\"AVG\",\"column\":\"amount\",\"outputName\":\"avg\"},"
                + "  {\"function\":\"MIN\",\"column\":\"amount\",\"outputName\":\"min_amt\"},"
                + "  {\"function\":\"MAX\",\"column\":\"amount\",\"outputName\":\"max_amt\"}"
                + "]}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size(),
                "combine mode must register exactly one endpoint, not one per surviving file");

        Map<String, long[]> byLevel = new HashMap<>(); // level -> [cnt, total, min, max], avg checked separately
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

        assertEquals(2, byLevel.size(), "two groups -- one per level, combined across both partition files' own single file each");
        assertArrayEquals(new long[] {5, 15, 1, 5}, byLevel.get("ERROR"));
        assertArrayEquals(new long[] {5, 510, 100, 104}, byLevel.get("INFO"));
        assertEquals(3.0, avgByLevel.get("ERROR"), 0.001);
        assertEquals(102.0, avgByLevel.get("INFO"), 0.001);
    }

    @Test
    void orderByLimit_withNoGroupBy_acrossMultipleFiles_returnsGlobalTopN(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot); // amounts 1..5 and 100..104, 10 rows total across 2 files

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"columns\":[\"amount\"],"
                + "\"orderBy\":[{\"column\":\"amount\",\"ascending\":false}],"
                + "\"limit\":3}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size());

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

        // The global top 3 by amount is 104, 103, 102 (from the INFO file) --
        // a per-file-local top-3 would wrongly include ERROR-partition rows
        // (max 5) since each file only has 5 rows.
        assertEquals(List.of(104L, 103L, 102L), amounts,
                "must be the true global top-3 across both files, not a per-file-local one");
    }

    @Test
    void groupByWithOrderByOnAggregateOutput_andLimit_allTogether(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"groupBy\":[\"level\"],"
                + "\"aggregates\":[{\"function\":\"SUM\",\"column\":\"amount\",\"outputName\":\"total\"}],"
                + "\"orderBy\":[{\"column\":\"total\",\"ascending\":false}],"
                + "\"limit\":1}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        List<String> levels = new java.util.ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    assertEquals(1, root.getRowCount(), "limit=1 after grouping+ordering must leave exactly one row");
                    VarCharVector levelVector = (VarCharVector) root.getVector("level");
                    levels.add(new String(levelVector.get(0), StandardCharsets.UTF_8));
                }
            }
        }
        assertEquals(List.of("INFO"), levels, "INFO has the higher total (510 vs 15), ordered descending, limit 1");
    }

    @Test
    void groupBy_unknownColumn_returnsClearInvalidArgument(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"groupBy\":[\"does_not_exist\"],"
                + "\"aggregates\":[{\"function\":\"COUNT\",\"column\":\"amount\",\"outputName\":\"cnt\"}]}";

        FlightRuntimeException thrown = assertThrows(FlightRuntimeException.class,
                () -> client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8))));
        assertTrue(thrown.getMessage().contains("does_not_exist"),
                "error must name the missing column: " + thrown.getMessage());
    }
}
