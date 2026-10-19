package com.labuk.labukdb.core.decode;

import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unlike the log/ and planner/ tests, which run against fixed JSON
 * fixtures, this one writes a real Parquet file with parquet-mr's own
 * writer and reads it back through {@link ParquetToArrowReader} — the
 * closest thing to an end-to-end check available without a running Delta
 * table.
 *
 * NOTE ON SCOPE: 20 rows is nowhere near enough to force parquet-mr to
 * split across multiple row groups (the default check interval is every
 * 100 records before it even measures size), so this does not distinctly
 * exercise {@link RowGroupPruner}'s "skip an entire row group" path — it
 * exercises the single-row-group case, plus the fused decode+residual-filter
 * loop, which is the part most likely to have an off-by-one or type-mapping
 * bug. Row-group-level range pruning shares its comparison logic with
 * {@code FileStatsPruner}, already covered directly in {@code PrunerTest}.
 */
class ParquetToArrowReaderTest {

    private static final String SCHEMA_STRING =
            "message test { required int64 amount; required binary label (UTF8); }";

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private File writeSampleFile(Path tempDir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STRING);
        File file = tempDir.resolve("sample.parquet").toFile();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (long i = 0; i < 20; i++) {
                Group group = factory.newGroup()
                        .append("amount", i)
                        .append("label", i % 2 == 0 ? "even" : "odd");
                writer.write(group);
            }
        }
        return file;
    }

    @Test
    void readsAllRowsWithNoPredicate(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, null, null)) {
            assertEquals(20, root.getRowCount());
        }
    }

    @Test
    void residualFilter_appliesRowLevelPredicate(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 15L);
        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, null, predicate)) {
            assertEquals(4, root.getRowCount(), "amount > 15 out of 0..19 should leave {16,17,18,19}");

            BigIntVector amounts = (BigIntVector) root.getVector("amount");
            for (int i = 0; i < root.getRowCount(); i++) {
                assertTrue(amounts.get(i) > 15);
            }
        }
    }

    @Test
    void projection_onlyDecodesRequestedColumns(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, List.of("label"), null)) {
            assertEquals(1, root.getSchema().getFields().size());
            assertEquals("label", root.getSchema().getFields().get(0).getName());
            assertNull(root.getVector("amount"), "unrequested column must not appear in the result at all");
        }
    }

    @Test
    void stringPredicate_matchesExpectedSubset(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        Predicate predicate = Predicate.compare("label", ComparisonOp.EQ, "even");
        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, null, predicate)) {
            assertEquals(10, root.getRowCount());
            VarCharVector labels = (VarCharVector) root.getVector("label");
            for (int i = 0; i < root.getRowCount(); i++) {
                assertEquals("even", new String(labels.get(i)));
            }
        }
    }

    @Test
    void readSchema_doesNotRequireOpeningRowGroups(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        var schema = new ParquetToArrowReader().readSchema(file, null);
        assertEquals(2, schema.getFields().size());
    }

    @Test
    void partitionColumn_reconstructedAsConstant_whenExplicitlyRequested(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);
        Map<String, String> partitionValues = Map.of("region", "eu");

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(
                file, allocator, List.of("label", "region"), null, partitionValues)) {
            assertEquals(20, root.getRowCount());
            VarCharVector regionVector = (VarCharVector) root.getVector("region");
            for (int i = 0; i < root.getRowCount(); i++) {
                assertEquals("eu", new String(regionVector.get(i)),
                        "partition column must be the same constant on every row");
            }
        }
    }

    @Test
    void partitionColumn_includedByDefault_whenNoExplicitProjectionGiven(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);
        Map<String, String> partitionValues = Map.of("region", "eu");

        // null projection means "select *" — for a Hive-partitioned table
        // that must include the partition column, not just the file's own
        // physical fields.
        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, null, null, partitionValues)) {
            assertEquals(3, root.getSchema().getFields().size(), "amount, label, and region");
            assertNotNull(root.getVector("region"));
        }
    }

    @Test
    void requestingUnknownColumn_throwsRatherThanSilentlyProducingWrongData(@TempDir Path tempDir) throws Exception {
        File file = writeSampleFile(tempDir);

        assertThrows(IllegalArgumentException.class, () ->
                new ParquetToArrowReader().read(file, allocator, List.of("does_not_exist"), null, Map.of()));
    }

    @Test
    void inputFileBasedPath_matchesFileBasedPath_forTheSameFile(@TempDir Path tempDir) throws Exception {
        // Proves the storage-abstracted core (InputFile) produces identical
        // results to the original File-based path, for the same
        // underlying file — the decode-layer equivalent of
        // HadoopBackedSnapshotManagerTest's local-equivalence check. Does
        // NOT prove real S3/MinIO/ADLS works — there's no real endpoint to
        // test against here.
        File file = writeSampleFile(tempDir);
        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 15L);

        try (VectorSchemaRoot viaFile = new ParquetToArrowReader().read(file, allocator, null, predicate);
             VectorSchemaRoot viaInputFile = new ParquetToArrowReader().read(
                     ParquetInputFiles.local(file), allocator, null, predicate, Map.of())) {

            assertEquals(viaFile.getRowCount(), viaInputFile.getRowCount());
            assertEquals(viaFile.getSchema(), viaInputFile.getSchema());

            BigIntVector fileAmounts = (BigIntVector) viaFile.getVector("amount");
            BigIntVector inputFileAmounts = (BigIntVector) viaInputFile.getVector("amount");
            for (int i = 0; i < viaFile.getRowCount(); i++) {
                assertEquals(fileAmounts.get(i), inputFileAmounts.get(i));
            }
        }
    }
}
