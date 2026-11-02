package com.labuk.labukdb.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the CLI's {@code --sql} flag genuinely combines rows across
 * multiple physical files via {@link QueryCommand}'s shared
 * {@code CombinedQueryExecutor} path — the same cross-file-combine
 * correctness bar {@code FlightAggregationIntegrationTest} already holds
 * Flight's {@code DoGet} to, now proven on the embedded CLI path too (the
 * actual gap that motivated extracting {@code CombinedQueryExecutor} into
 * {@code labukdb-core} in the first place).
 *
 * The catalog is isolated from the real {@code ~/.labukdb/catalog.json} by
 * pointing {@code user.home} at a fresh {@code @TempDir} for the duration
 * of each test, restored in {@code @AfterEach} — {@link QueryCommand}
 * always resolves {@code TableCatalog.atDefaultLocation()} from
 * {@code user.home}, with no test-only override hook, and a real CLI
 * command must never touch a developer's actual catalog file.
 */
class QueryCommandSqlIntegrationTest {

    private static final String FILE_SCHEMA_STRING =
            "message test { required int64 amount; required binary message (UTF8); }";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void setUp(@TempDir Path fakeHome) {
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setProperty("user.home", originalUserHome);
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
                "{\"metaData\":{\"id\":\"cli-sql-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + MAPPER.writeValueAsString(schemaString) + ","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}",
                "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"ERROR\"},"
                        + "\"size\":" + errorFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"level=INFO/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"INFO\"},"
                        + "\"size\":" + infoFileSize + ",\"modificationTime\":1732000000500,\"dataChange\":true}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    /** ERROR: amounts 1..5 (sum 15). INFO: amounts 100..104 (sum 510). */
    private void buildTwoPartitionFixture(Path tableRoot) throws Exception {
        writePartitionFile(tableRoot.resolve("level=ERROR"), "ERROR", 1, 5);
        writePartitionFile(tableRoot.resolve("level=INFO"), "INFO", 100, 5);
        writeCommitLog(tableRoot);
    }

    @Test
    void sqlGroupByCount_combinesAcrossMultipleFiles(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);
        TableCatalog.atDefaultLocation().register("logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("level | cnt"), output);
        assertTrue(output.contains("ERROR | 5"), "ERROR partition's 5 rows must combine into one count: " + output);
        assertTrue(output.contains("INFO | 5"), "INFO partition's 5 rows must combine into one count: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    /**
     * {@code MIN}/{@code MAX} are fully implemented and already tested via
     * {@code AggregateOperatorTest}/Flight's JSON-request path, but had
     * zero coverage through actual SQL text until this session's SQL
     * engine Phase 1 work found the gap — closing it here.
     */
    @Test
    void sqlGroupByMinMax_combinesAcrossMultipleFiles(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot); // ERROR: amounts 1..5. INFO: amounts 100..104.
        TableCatalog.atDefaultLocation().register("logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT level, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM logs GROUP BY level"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("level | min_amt | max_amt"), output);
        assertTrue(output.contains("ERROR | 1 | 5"), "ERROR partition's true min/max across all 5 rows: " + output);
        assertTrue(output.contains("INFO | 100 | 104"), "INFO partition's true min/max across all 5 rows: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void sqlOrderByLimit_withNoGroupBy_returnsGlobalTopN(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot); // amounts 1..5 and 100..104 across two files

        TableCatalog.atDefaultLocation().register("logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        // Data rows only, after the "amount\n---..." header -- a plain
        // output.indexOf("104") search is fragile here: @TempDir's own
        // randomly-generated numeric directory name can coincidentally
        // contain "104"/"103"/"102" as a substring earlier in the output
        // than the real data (observed in practice, not hypothetical).
        List<String> dataLines = output.lines()
                .dropWhile(line -> !line.startsWith("---"))
                .skip(1)
                .takeWhile(line -> !line.isBlank())
                .toList();
        assertEquals(List.of("104", "103", "102"), dataLines,
                "must list the true global top-3 (104, 103, 102), not a per-file-local one: " + output);
        assertTrue(output.contains("3 row(s)"), output);
    }

    @Test
    void sqlWithWhere_noCombineModeNeeded_filtersCorrectly(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);
        TableCatalog.atDefaultLocation().register("logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT amount, message FROM logs WHERE level = 'ERROR'"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("5 row(s)"), "only the ERROR partition's 5 rows should match: " + output);
        assertFalse(output.contains("info-msg"), "INFO partition rows must be excluded by the WHERE clause: " + output);
    }

    // Not exercised through QueryCommand.run itself: CliArgs.parse() returning null
    // there leads straight into System.exit(1), which would kill the test JVM.
    // Asserted directly against CliArgs.parse instead.
    @Test
    void cliArgsParse_sqlMutuallyExclusiveWithTable_returnsNull() {
        assertNull(CliArgs.parse(new String[] {"--sql", "SELECT x FROM y", "--table", "logs"}));
    }

    @Test
    void cliArgsParse_sqlMutuallyExclusiveWithLimit_returnsNull() {
        assertNull(CliArgs.parse(new String[] {"--sql", "SELECT x FROM y", "--limit", "10"}));
    }
}
