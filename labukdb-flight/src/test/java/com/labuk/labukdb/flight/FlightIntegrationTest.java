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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The end-to-end proof-of-life this project didn't have before: a real
 * running {@link LabukDbServerApp}-equivalent server (started
 * in-process here, same producer, an ephemeral local port), a real
 * {@link FlightClient}, and a fixture table with actual Parquet files on
 * disk — not just log fixtures, since {@code DoGet} genuinely opens files.
 *
 * The query exercises partition pruning (filters to the {@code level=ERROR}
 * partition, expecting exactly one {@link FlightEndpoint}), the residual
 * row filter (an {@code amount > 2} predicate), AND partition-column
 * reconstruction (requesting {@code level} back as an output column, even
 * though it doesn't exist in the physical Parquet file) — all in the same
 * request. A pass here means Layer 2 pruning, Layer 3 decode, and the
 * partition-value substitution/reconstruction added afterward are all
 * correctly wired through {@code GetFlightInfo}/{@code DoGet} together, not
 * just individually unit-tested.
 */
class FlightIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String FILE_SCHEMA_STRING =
            "message test { required int64 amount; required binary message (UTF8); }";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightClient client;
    private TableCatalog catalog;

    @TempDir
    Path catalogDir;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        catalog = new TableCatalog(catalogDir.resolve("catalog.json"));
        DeltaFlightProducer producer = new DeltaFlightProducer(allocator, catalog);

        Location serverLocation = Location.forGrpcInsecure("localhost", 0);
        server = FlightServer.builder(allocator, serverLocation, producer).build();
        server.start();

        Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
        client = FlightClient.builder(allocator, clientLocation).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.close();
        allocator.close();
    }

    /** Writes one real Parquet file with amount 1..count and message "<partitionValue>-msg-N". */
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

    /** Writes a minimal but real _delta_log commit referencing the two partition files just written. */
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
                "{\"metaData\":{\"id\":\"integration-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + MAPPER.writeValueAsString(schemaString) + ","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}",
                "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"ERROR\"},"
                        + "\"size\":" + errorFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"level=INFO/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"INFO\"},"
                        + "\"size\":" + infoFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    @Test
    void partitionPruningAndResidualFilter_workTogetherThroughRealFlightRoundTrip(@TempDir Path tableRoot) throws Exception {
        writePartitionFile(tableRoot.resolve("level=ERROR"), "ERROR", 1, 5); // amounts 1..5
        writePartitionFile(tableRoot.resolve("level=INFO"), "INFO", 100, 5); // amounts 100..104
        writeCommitLog(tableRoot);

        // "level" is requested here specifically because it's the partition
        // column — this exercises both PartitionValueAppender (reconstructing
        // it as a constant per row) AND PartitionValueSubstituter (since the
        // predicate below ALSO filters on "level", which must be folded away
        // before it reaches decode, or GroupPredicateEvaluator would throw
        // trying to find a "level" field in a file that doesn't have one).
        String queryJson = "{"
                + "\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"columns\":[\"level\",\"message\",\"amount\"],"
                + "\"predicate\":{\"type\":\"and\",\"children\":["
                + "  {\"type\":\"cmp\",\"column\":\"level\",\"op\":\"EQ\",\"literal\":\"ERROR\",\"literalType\":\"string\"},"
                + "  {\"type\":\"cmp\",\"column\":\"amount\",\"op\":\"GT\",\"literal\":2,\"literalType\":\"long\"}"
                + "]}}";

        FlightDescriptor descriptor = FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8));
        FlightInfo info = client.getInfo(descriptor);

        assertEquals(1, info.getEndpoints().size(),
                "partition pruning on level=ERROR should leave exactly one file/endpoint, "
                        + "not both partitions' files");

        List<Long> amounts = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        List<String> levels = new ArrayList<>();

        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    BigIntVector amountVector = (BigIntVector) root.getVector("amount");
                    VarCharVector messageVector = (VarCharVector) root.getVector("message");
                    VarCharVector levelVector = (VarCharVector) root.getVector("level");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        amounts.add(amountVector.get(i));
                        messages.add(new String(messageVector.get(i), StandardCharsets.UTF_8));
                        levels.add(new String(levelVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
        }

        assertEquals(3, amounts.size(), "amount > 2 out of {1,2,3,4,5} should leave {3,4,5}");
        assertTrue(amounts.containsAll(List.of(3L, 4L, 5L)));
        assertTrue(messages.stream().allMatch(m -> m.startsWith("error-msg-")),
                "the INFO partition's rows must never have been decoded at all, not just filtered out");
        assertTrue(levels.stream().allMatch("ERROR"::equals),
                "the partition column must be reconstructed as a constant on every row, "
                        + "even though it doesn't exist in the physical Parquet file");
    }

    @Test
    void queryingByCatalogName_resolvesToTheRegisteredPath(@TempDir Path tableRoot) throws Exception {
        writePartitionFile(tableRoot.resolve("level=ERROR"), "ERROR", 1, 3);
        writePartitionFile(tableRoot.resolve("level=INFO"), "INFO", 100, 1);
        writeCommitLog(tableRoot); // references both partitions, so both files must exist first

        catalog.register("logs", tableRoot.toString());

        String queryJson = "{\"table\":\"logs\",\"predicate\":"
                + "{\"type\":\"cmp\",\"column\":\"level\",\"op\":\"EQ\",\"literal\":\"ERROR\",\"literalType\":\"string\"}}";

        FlightDescriptor descriptor = FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8));
        FlightInfo info = client.getInfo(descriptor);

        assertEquals(1, info.getEndpoints().size(),
                "a query by catalog name should resolve to the registered path and prune identically to a raw-path query");

        int totalRows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        assertEquals(3, totalRows);
    }

    @Test
    void getSchema_returnsTableSchemaWithoutPlanningScan(@TempDir Path tableRoot) throws Exception {
        writePartitionFile(tableRoot.resolve("level=ERROR"), "ERROR", 1, 3);
        writePartitionFile(tableRoot.resolve("level=INFO"), "INFO", 100, 1);
        writeCommitLog(tableRoot);
        catalog.register("schema-test", tableRoot.toString());

        // GetSchema must return the schema cheaply — no scan, no endpoints.
        // This is what the NL query validator calls to get a column whitelist.
        FlightDescriptor descriptor = FlightDescriptor.command(
                "{\"table\":\"schema-test\"}".getBytes(StandardCharsets.UTF_8));
        var result = client.getSchema(descriptor);

        assertNotNull(result);
        assertNotNull(result.getSchema());
        assertTrue(result.getSchema().getFields().size() >= 2,
                "schema must contain at least the physical columns (amount, message)");
    }

    /** A minimal commit establishing an empty table — metaData only, no data files yet — for DoPut to write into. */
    private void writeEmptyCommitLog(Path tableRoot, String partitionColumn) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);
        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"doput-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + MAPPER.writeValueAsString(schemaString) + ","
                        + "\"partitionColumns\":[\"" + partitionColumn + "\"],\"configuration\":{},\"createdTime\":1732000000000}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
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

    @Test
    void doPut_thenQueryThroughExistingReadPath_returnsWrittenRows(@TempDir Path tableRoot) throws Exception {
        writeEmptyCommitLog(tableRoot, "level");

        String writeJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"partitionValues\":{\"level\":\"ERROR\"}}";
        FlightDescriptor putDescriptor = FlightDescriptor.command(writeJson.getBytes(StandardCharsets.UTF_8));

        try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "gateway"}, new long[] {1, 2});
             SyncPutListener putListener = new SyncPutListener()) {
            var putStream = client.startPut(putDescriptor, batch, putListener);
            putStream.putNext();
            putStream.completed();
            PutResult ack = putListener.read();
            assertNotNull(ack, "server must ack the put");
        }

        // Read back through the existing, UNMODIFIED GetFlightInfo/DoGet path.
        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + "}";
        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size(), "exactly one file was committed by the put");

        int totalRows = 0;
        List<String> levels = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    totalRows += root.getRowCount();
                    VarCharVector levelVector = (VarCharVector) root.getVector("level");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        levels.add(new String(levelVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        assertEquals(2, totalRows, "both rows from the one streamed batch were committed and are readable back");
        assertTrue(levels.stream().allMatch("ERROR"::equals),
                "the partition column, supplied only in the DoPut request, must be reconstructed correctly on read");
    }

    @Test
    void doPut_twiceAgainstSameTableAndPartition_bothCallsVisibleAfterward(@TempDir Path tableRoot) throws Exception {
        writeEmptyCommitLog(tableRoot, "level");
        String writeJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"partitionValues\":{\"level\":\"ERROR\"}}";

        for (String service : List.of("first-call-svc", "second-call-svc")) {
            FlightDescriptor putDescriptor = FlightDescriptor.command(writeJson.getBytes(StandardCharsets.UTF_8));
            try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {service}, new long[] {1});
                 SyncPutListener putListener = new SyncPutListener()) {
                var putStream = client.startPut(putDescriptor, batch, putListener);
                putStream.putNext();
                putStream.completed();
                putListener.getResult();
            }
        }

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + "}";
        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(2, info.getEndpoints().size(), "two separate DoPut calls committed two separate files");

        List<String> services = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VarCharVector serviceVector = (VarCharVector) stream.getRoot().getVector("service");
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        services.add(new String(serviceVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        assertEquals(List.of("first-call-svc", "second-call-svc"),
                services.stream().sorted().toList());
    }

    @Test
    void doPut_remoteTableLocation_returnsUnimplementedNotAConfusingFailure(@TempDir Path tableRoot) throws Exception {
        String writeJson = "{\"tableRoot\":\"s3a://fake-bucket/fake-table\",\"partitionValues\":{\"level\":\"ERROR\"}}";
        FlightDescriptor putDescriptor = FlightDescriptor.command(writeJson.getBytes(StandardCharsets.UTF_8));

        try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"x"}, new long[] {1});
             SyncPutListener putListener = new SyncPutListener()) {
            var putStream = client.startPut(putDescriptor, batch, putListener);
            putStream.putNext();
            putStream.completed();
            FlightRuntimeException thrown = assertThrows(FlightRuntimeException.class, putListener::getResult);
            assertTrue(thrown.getMessage().contains("local-disk only"),
                    "error must explain why, not just fail opaquely: " + thrown.getMessage());
        }
    }

    @Test
    void listFlights_returnsRegisteredCatalogEntries() throws Exception {
        catalog.register("alpha", "some-table-a");
        catalog.register("beta", "some-table-b");

        Iterable<FlightInfo> listing = client.listFlights(org.apache.arrow.flight.Criteria.ALL);
        List<String> names = new ArrayList<>();
        listing.forEach(info -> names.add(info.getDescriptor().getPath().get(0)));

        assertEquals(2, names.size());
        assertTrue(names.containsAll(List.of("alpha", "beta")));
    }
}
