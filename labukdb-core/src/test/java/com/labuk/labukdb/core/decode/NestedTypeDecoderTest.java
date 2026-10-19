package com.labuk.labukdb.core.decode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
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
 * Tests for nested LIST type support — specifically the embedding vector
 * use case that lakehouse-gateway's hybrid search integration requires.
 *
 * A Delta table written by Spark ML typically stores embedding vectors as
 * {@code ARRAY<FLOAT>} or {@code ARRAY<DOUBLE>} columns. Without nested
 * type support, any attempt to read such a table would throw
 * {@code UnsupportedOperationException} at schema-mapping time.
 */
class NestedTypeDecoderTest {

    // parquet-mr 1.13.1 uses OriginalType enum — the schema parser
    // requires the older annotation syntax (LIST not List, no parentheses
    // around the inner group repetition). This is the 3-level LIST encoding
    // Spark actually writes for ARRAY<FLOAT> columns.
    private static final String SCHEMA_STRING = """
            message embedding_table {
              required binary id (UTF8);
              optional group embedding (LIST) {
                repeated group list {
                  optional float element;
                }
              }
            }
            """;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private File writeEmbeddingFile(Path dir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STRING);
        File file = dir.resolve("embeddings.snappy.parquet").toFile();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);

            // Row 1: id="doc-1", embedding=[0.1, 0.2, 0.3]
            Group row1 = factory.newGroup().append("id", "doc-1");
            Group embGroup1 = row1.addGroup("embedding");
            Group listEntry;
            listEntry = embGroup1.addGroup("list");
            listEntry.append("element", 0.1f);
            listEntry = embGroup1.addGroup("list");
            listEntry.append("element", 0.2f);
            listEntry = embGroup1.addGroup("list");
            listEntry.append("element", 0.3f);
            writer.write(row1);

            // Row 2: id="doc-2", embedding=[0.4, 0.5, 0.6]
            Group row2 = factory.newGroup().append("id", "doc-2");
            Group embGroup2 = row2.addGroup("embedding");
            listEntry = embGroup2.addGroup("list");
            listEntry.append("element", 0.4f);
            listEntry = embGroup2.addGroup("list");
            listEntry.append("element", 0.5f);
            listEntry = embGroup2.addGroup("list");
            listEntry.append("element", 0.6f);
            writer.write(row2);
        }
        return file;
    }

    @Test
    void readsEmbeddingVectorColumn_asArrowListVector(@TempDir Path tempDir) throws Exception {
        File file = writeEmbeddingFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader()
                .read(file, allocator, null, null, Map.of())) {

            assertEquals(2, root.getRowCount());
            assertEquals(2, root.getSchema().getFields().size(),
                    "schema should contain id + embedding");

            ListVector embeddingVector = (ListVector) root.getVector("embedding");
            assertNotNull(embeddingVector);

            // Row 0: [0.1, 0.2, 0.3]
            List<?> row0 = (List<?>) embeddingVector.getObject(0);
            assertEquals(3, row0.size());
            assertEquals(0.1f, ((Number) row0.get(0)).floatValue(), 0.0001f);
            assertEquals(0.2f, ((Number) row0.get(1)).floatValue(), 0.0001f);
            assertEquals(0.3f, ((Number) row0.get(2)).floatValue(), 0.0001f);

            // Row 1: [0.4, 0.5, 0.6]
            List<?> row1 = (List<?>) embeddingVector.getObject(1);
            assertEquals(3, row1.size());
            assertEquals(0.4f, ((Number) row1.get(0)).floatValue(), 0.0001f);
        }
    }

    @Test
    void projectingEmbeddingColumnOnly_returnsJustEmbedding(@TempDir Path tempDir) throws Exception {
        File file = writeEmbeddingFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader()
                .read(file, allocator, List.of("embedding"), null, Map.of())) {

            assertEquals(2, root.getRowCount());
            assertEquals(1, root.getSchema().getFields().size(),
                    "only the embedding column should be present");
            assertNull(root.getVector("id"),
                    "unprojected columns must not appear in the output");
        }
    }

    @Test
    void mixedPrimitiveAndNestedColumns_bothDecodedCorrectly(@TempDir Path tempDir) throws Exception {
        File file = writeEmbeddingFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader()
                .read(file, allocator, List.of("id", "embedding"), null, Map.of())) {

            assertEquals(2, root.getRowCount());
            assertEquals(2, root.getSchema().getFields().size());
            assertNotNull(root.getVector("id"));
            assertNotNull(root.getVector("embedding"));
        }
    }
}
