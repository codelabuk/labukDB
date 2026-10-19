package com.labuk.labukdb.core.decode;

import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
 * Regression tests for two bugs found in real usage against the
 * job-tracker-silver Delta table:
 *
 * Bug 1 — Predicate column excluded from projection:
 * A query with columns:["company","title"] and predicate on is_active
 * caused InvalidRecordException because is_active was never decoded.
 * Fix: physical read set = output columns UNION predicate columns;
 * predicate-only columns are dropped before returning to the caller.
 *
 * Bug 2 — Boolean literal type missing from PredicateCodec:
 * literalType:"boolean" was not handled, so boolean comparisons fell
 * through to string comparison and never matched real boolean values.
 */
class ProjectionPredicateBugTest {

    private static final String SCHEMA_STRING =
            "message test { required binary company (UTF8); required binary title (UTF8); required boolean is_active; }";

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private File writeFile(Path dir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STRING);
        File file = dir.resolve("part-0.snappy.parquet").toFile();
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("company", "Acme").append("title", "Engineer").append("is_active", true));
            writer.write(factory.newGroup().append("company", "Globex").append("title", "Analyst").append("is_active", false));
            writer.write(factory.newGroup().append("company", "Initech").append("title", "Manager").append("is_active", true));
        }
        return file;
    }

    @Test
    void bug1_predicateColumnExcludedFromProjection_noLongerThrows(@TempDir Path tempDir) throws Exception {
        // Reproduces: columns:["company","title"] with predicate on is_active
        // Previously: InvalidRecordException: is_active not found in schema
        // Now: is_active is read for filtering, dropped from output
        File file = writeFile(tempDir);
        Predicate predicate = Predicate.compare("is_active", ComparisonOp.EQ, false);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(
                file, allocator, List.of("company", "title"), predicate, Map.of())) {

            assertEquals(1, root.getRowCount(),
                    "only the one inactive row (Globex) should survive");
            assertEquals(2, root.getSchema().getFields().size(),
                    "output must only contain the 2 requested columns, not is_active");
            assertNull(root.getVector("is_active"),
                    "is_active was predicate-only — must not appear in the output");

            VarCharVector companies = (VarCharVector) root.getVector("company");
            assertEquals("Globex", new String(companies.get(0)));
        }
    }

    @Test
    void bug1_predicateColumnAlreadyInProjection_stillWorks(@TempDir Path tempDir) throws Exception {
        // Existing behavior must not regress: when is_active IS in the
        // requested columns, it should appear in the output as before.
        File file = writeFile(tempDir);
        Predicate predicate = Predicate.compare("is_active", ComparisonOp.EQ, false);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(
                file, allocator, List.of("company", "title", "is_active"), predicate, Map.of())) {

            assertEquals(1, root.getRowCount());
            assertEquals(3, root.getSchema().getFields().size());
            assertNotNull(root.getVector("is_active"),
                    "is_active was explicitly requested — must appear in the output");
        }
    }

    @Test
    void bug1_noProjection_allColumnsIncludingPredicateColumn(@TempDir Path tempDir) throws Exception {
        File file = writeFile(tempDir);
        Predicate predicate = Predicate.compare("is_active", ComparisonOp.EQ, true);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(
                file, allocator, null, predicate, Map.of())) {

            assertEquals(2, root.getRowCount(), "Acme and Initech are active");
            assertEquals(3, root.getSchema().getFields().size());
        }
    }

    @Test
    void bug2_booleanLiteralType_matchesRealBooleanColumn(@TempDir Path tempDir) throws Exception {
        // Reproduces: literalType:"boolean" was missing from PredicateCodec,
        // causing boolean comparisons to fall through to string comparison
        // and never match real boolean values in the Parquet file.
        File file = writeFile(tempDir);

        // Simulate what PredicateCodec now produces for literalType:"boolean"
        Predicate predicate = Predicate.compare("is_active", ComparisonOp.EQ, false);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(
                file, allocator, List.of("company"), predicate, Map.of())) {

            assertEquals(1, root.getRowCount(),
                    "Boolean false must match the one inactive row — "
                            + "if this is 0, the boolean literal is being compared as a string");
            VarCharVector companies = (VarCharVector) root.getVector("company");
            assertEquals("Globex", new String(companies.get(0)));
        }
    }
}
