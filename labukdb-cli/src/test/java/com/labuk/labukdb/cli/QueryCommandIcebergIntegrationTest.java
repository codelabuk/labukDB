package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.iceberg.IcebergTableFormat;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the real point of this pass: an Iceberg table is genuinely
 * reachable through {@code --table}/{@code --filter} and {@code --sql} on
 * the CLI, not just through {@link IcebergTableFormat} directly in Java —
 * that was the honesty gap this closes (confirmed via grep before this
 * change: zero references to Iceberg existed anywhere in {@code labukdb-cli}
 * or {@code labukdb-flight}). Same real-table-fixture rigor
 * {@code IcebergTableFormatTest} already uses (real {@code HadoopTables}
 * creation, real committed Parquet files via the unmodified {@code
 * ArrowToParquetWriter}), extended to two partition files so {@code --sql}'s
 * {@code GROUP BY} genuinely proves cross-file combination, the same bar
 * {@code QueryCommandSqlIntegrationTest} holds the Delta path to.
 */
class QueryCommandIcebergIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

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

    private void writePartitionFile(Table table, Path tableRoot, String level, int startAmount, int count) throws Exception {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator)) {
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
    void flagBasedFilter_readsRealIcebergTable(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);
        TableCatalog.atDefaultLocation().register("iceberg_logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--table", "iceberg_logs", "--filter", "level=ERROR",
                "--columns", "service,amount"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("Iceberg snapshot"), "must report it detected and read this as Iceberg: " + output);
        assertTrue(output.contains("5 row(s)"), "only the ERROR partition's 5 rows should match: " + output);
        assertFalse(output.contains("info-svc"), "INFO partition rows must be excluded by the filter: " + output);
    }

    @Test
    void sqlGroupByCount_combinesAcrossMultipleIcebergFiles(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);
        TableCatalog.atDefaultLocation().register("iceberg_logs", tableRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT level, COUNT(amount) AS cnt FROM iceberg_logs GROUP BY level"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("level | cnt"), output);
        assertTrue(output.contains("ERROR | 5"), "ERROR partition's 5 rows must combine into one count: " + output);
        assertTrue(output.contains("INFO | 5"), "INFO partition's 5 rows must combine into one count: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void sqlPlainSelect_noGroupByOrLimit_stillReadsAllRows(@TempDir Path tableRoot) throws Exception {
        buildTwoPartitionFixture(tableRoot);
        TableCatalog.atDefaultLocation().register("iceberg_logs", tableRoot.toAbsolutePath().toString());

        // No GROUP BY/ORDER BY/LIMIT -- exercises Iceberg's deliberate
        // "always combine" path (unlike Delta, which would stay on the
        // per-file streaming path for a query this plain).
        QueryCommand.run(new String[] {"--sql", "SELECT amount FROM iceberg_logs"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("10 row(s)"), "all 10 rows across both partition files: " + output);
    }
}
