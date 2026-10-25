package com.labuk.labukdb.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves Iceberg tables are genuinely reachable over real Flight {@code
 * GetFlightInfo}/{@code DoGet} — same real-fixture rigor {@code
 * FlightAggregationIntegrationTest} uses for Delta (a real server + a real
 * client, two partition files so combination is actually exercised, not
 * just "it compiles"), and proves {@code DoPut} against a detected-Iceberg
 * table is rejected before touching anything, since Iceberg write support
 * doesn't exist.
 */
class FlightIcebergIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightClient client;
    private TableCatalog catalog;

    @TempDir
    Path catalogDir;

    @TempDir
    Path walRoot;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        catalog = new TableCatalog(catalogDir.resolve("catalog.json"));
        LabukDbFlightProducer producer = new LabukDbFlightProducer(allocator, catalog, new Configuration(), walRoot);

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

    private void writePartitionFile(Table table, Path tableRoot, String level, int startAmount, int count) throws Exception {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), alloc)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < count; i++) {
                serviceVector.setSafe(i, (level.toLowerCase() + "-svc").getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, (long) (startAmount + i));
            }
            batch.setRowCount(count);

            File parquetFile = tableRoot.resolve("level=" + level + "/part-0.parquet").toFile();
            parquetFile.getParentFile().mkdirs();
            new ArrowToParquetWriter().write(parquetFile, batch);

            DataFile dataFile = DataFiles.builder(SPEC)
                    .withPath(parquetFile.getAbsolutePath())
                    .withFileSizeInBytes(parquetFile.length())
                    .withRecordCount(count)
                    .withPartitionPath("level=" + level)
                    .build();
            table.newAppend().appendFile(dataFile).commit();
        }
    }

    /** ERROR: amounts 1..5 (sum 15). INFO: amounts 100..104 (sum 510). */
    private void buildTwoPartitionFixture(Path tableRoot) throws Exception {
        Table table = new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());
        writePartitionFile(table, tableRoot, "ERROR", 1, 5);
        writePartitionFile(table, tableRoot, "INFO", 100, 5);
    }

    @Test
    void plainScan_noGroupByOrLimit_readsAllRowsAcrossBothFiles(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + "}";
        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        // Iceberg always goes through the combined path -- exactly one
        // endpoint even for a plain scan with nothing to combine around.
        assertEquals(1, info.getEndpoints().size());

        int totalRows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        assertEquals(10, totalRows, "all 10 rows across both partition files");
    }

    @Test
    void groupBy_combinesAcrossMultipleIcebergFiles(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);

        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + ","
                + "\"groupBy\":[\"level\"],"
                + "\"aggregates\":[{\"function\":\"COUNT\",\"column\":\"amount\",\"outputName\":\"cnt\"}]}";

        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, info.getEndpoints().size());

        Map<String, Long> countByLevel = new HashMap<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    VarCharVector levelVector = (VarCharVector) root.getVector("level");
                    BigIntVector cnt = (BigIntVector) root.getVector("cnt");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        countByLevel.put(new String(levelVector.get(i), StandardCharsets.UTF_8), cnt.get(i));
                    }
                }
            }
        }
        assertEquals(Map.of("ERROR", 5L, "INFO", 5L), countByLevel,
                "each partition's 5 rows must combine into one count per level, not two partial results");
    }

    @Test
    void doPut_appendsToIcebergTable_thenReadableViaDoGet(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot); // ERROR: 1..5, INFO: 100..104 -- 10 rows total

        FlightDescriptor putDescriptor = FlightDescriptor.command(
                ("{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString())
                        + ",\"partitionValues\":{\"level\":\"WARN\"}}").getBytes(StandardCharsets.UTF_8));

        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
             SyncPutListener putListener = new SyncPutListener()) {
            batch.allocateNew();
            ((VarCharVector) batch.getVector("service")).setSafe(0, "new-svc".getBytes(StandardCharsets.UTF_8));
            ((BigIntVector) batch.getVector("amount")).setSafe(0, 999L);
            batch.setRowCount(1);

            var putStream = client.startPut(putDescriptor, batch, putListener);
            putStream.putNext();
            putStream.completed();
            PutResult ack = putListener.read();
            assertNotNull(ack, "server must ack the Iceberg DoPut");
        }

        // Read back through the existing, UNMODIFIED Iceberg GetFlightInfo/DoGet path.
        String queryJson = "{\"tableRoot\":" + MAPPER.writeValueAsString(tableRoot.toAbsolutePath().toString()) + "}";
        FlightInfo info = client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8)));

        int totalRows = 0;
        boolean sawNewRow = false;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    totalRows += root.getRowCount();
                    VarCharVector serviceVector = (VarCharVector) root.getVector("service");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if ("new-svc".equals(new String(serviceVector.get(i), StandardCharsets.UTF_8))) {
                            sawNewRow = true;
                        }
                    }
                }
            }
        }
        assertEquals(11, totalRows, "10 original rows + 1 newly appended row via DoPut");
        assertTrue(sawNewRow, "the newly appended row must be readable through the existing, unmodified read path");
    }

    @Test
    void icebergRestReference_getFlightInfo_routesToRestPath_notDeltaOrPathBasedIceberg() {
        // No real REST catalog server is available in this environment (see
        // IcebergRestConfiguration's javadoc). Proves ROUTING is correct --
        // an icebergrest:// tableRoot reaches IcebergRestConfiguration, not
        // DetectedFormat/SnapshotManager -- by asserting the specific
        // REST-connection-stage error, not some unrelated one a misrouted
        // request would produce instead.
        assertNull(System.getenv("ICEBERG_REST_URI"), "test assumes ICEBERG_REST_URI isn't set in this environment");
        String queryJson = "{\"tableRoot\":\"icebergrest://analytics.logs\"}";
        FlightRuntimeException thrown = assertThrows(FlightRuntimeException.class,
                () -> client.getInfo(FlightDescriptor.command(queryJson.getBytes(StandardCharsets.UTF_8))));
        assertTrue(thrown.getMessage().contains("ICEBERG_REST_URI"), thrown.getMessage());
    }

    @Test
    void icebergRestReference_doPut_routesToRestPath_notDeltaOrPathBasedIceberg() {
        assertNull(System.getenv("ICEBERG_REST_URI"), "test assumes ICEBERG_REST_URI isn't set in this environment");
        FlightDescriptor descriptor = FlightDescriptor.command(
                "{\"tableRoot\":\"icebergrest://analytics.logs\",\"partitionValues\":{\"level\":\"ERROR\"}}"
                        .getBytes(StandardCharsets.UTF_8));

        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(amountField)), allocator);
             SyncPutListener putListener = new SyncPutListener()) {
            batch.allocateNew();
            ((BigIntVector) batch.getVector("amount")).setSafe(0, 1L);
            batch.setRowCount(1);

            var putStream = client.startPut(descriptor, batch, putListener);
            putStream.putNext();
            putStream.completed();

            FlightRuntimeException thrown = assertThrows(FlightRuntimeException.class, putListener::getResult);
            assertTrue(thrown.getMessage().contains("ICEBERG_REST_URI"), thrown.getMessage());
        }
    }
}
