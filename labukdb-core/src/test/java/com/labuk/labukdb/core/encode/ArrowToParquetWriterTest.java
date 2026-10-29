package com.labuk.labukdb.core.encode;

import com.labuk.labukdb.core.decode.DecimalCodec;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * First test file for {@code encode/}. Covers {@link ArrowToParquetWriter}
 * writing a DECIMAL column (always this project's own 16-byte
 * {@code FIXED_LEN_BYTE_ARRAY} convention — see {@link DecimalCodec}) and
 * a full write-then-read round trip through {@link ParquetToArrowReader},
 * proving the two halves of this project's own codec agree with each
 * other, as a complement to {@code ParquetToArrowReaderDecimalTest}'s
 * proof that the read side also handles encodings this writer never
 * produces.
 */
class ArrowToParquetWriterTest {

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, int precision, int scale,
                                         int[] ids, BigDecimal[] prices) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field priceField = new Field("price", FieldType.nullable(DecimalCodec.toArrowType(precision, scale)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(idField, priceField)), allocator);
        root.allocateNew();

        IntVector idVector = (IntVector) root.getVector("id");
        DecimalVector priceVector = (DecimalVector) root.getVector("price");
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i]);
            if (prices[i] == null) {
                priceVector.setNull(i);
            } else {
                priceVector.setSafe(i, prices[i]);
            }
        }
        root.setRowCount(ids.length);
        return root;
    }

    @Test
    void writeThenRead_decimalColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        File file = new File(tempDir, "prices.parquet");
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, 10, 2,
                     new int[] {1, 2, 3},
                     new BigDecimal[] {new BigDecimal("10.50"), new BigDecimal("-1234.56"), new BigDecimal("0.00")})) {

            new ArrowToParquetWriter().write(file, batch);

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                assertEquals(3, result.getRowCount());
                DecimalVector price = (DecimalVector) result.getVector("price");
                assertEquals(new BigDecimal("10.50"), price.getObject(0));
                assertEquals(new BigDecimal("-1234.56"), price.getObject(1));
                assertEquals(new BigDecimal("0.00"), price.getObject(2));
            }
        }
    }

    @Test
    void writeThenRead_nullDecimalValue_roundTripsAsNull(@TempDir File tempDir) throws Exception {
        File file = new File(tempDir, "nulls.parquet");
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, 10, 2,
                     new int[] {1, 2},
                     new BigDecimal[] {new BigDecimal("5.00"), null})) {

            new ArrowToParquetWriter().write(file, batch);

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                DecimalVector price = (DecimalVector) result.getVector("price");
                assertFalse(price.isNull(0));
                assertTrue(price.isNull(1));
            }
        }
    }

    @Test
    void write_precisionOver38_rejectedAtSchemaBuildTime(@TempDir File tempDir) throws Exception {
        File file = new File(tempDir, "toobig.parquet");
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // ArrowType.Decimal itself doesn't validate precision -- construct
            // directly rather than through DecimalCodec.toArrowType, which
            // would reject it before the writer ever sees it.
            Field priceField = new Field("price",
                    FieldType.nullable(new ArrowType.Decimal(39, 2, 128)), null);
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(priceField)), allocator);
            batch.allocateNew();
            ((DecimalVector) batch.getVector("price")).setSafe(0, new BigDecimal("1.00"));
            batch.setRowCount(1);

            UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class,
                    () -> new ArrowToParquetWriter().write(file, batch));
            assertTrue(thrown.getMessage().contains("39"), thrown.getMessage());
            batch.close();
        }
    }

    /**
     * The write-side mirror of {@code NestedTypeDecoderTest} — same
     * embedding-vector use case, reversed: builds an Arrow
     * {@code ListVector<Float4>} directly (no hand-rolled Parquet
     * {@code Group} fixture needed, unlike the decode-side test, since
     * this writer is itself what now produces the Parquet structure) and
     * proves {@link ArrowToParquetWriter} and {@code NestedTypeDecoder}
     * agree on the exact same file, the same two-halves-agree proof
     * {@code writeThenRead_decimalColumn_roundTripsExactly} already gives
     * for DECIMAL.
     */
    private VectorSchemaRoot buildFloatListBatch(BufferAllocator allocator, float[][] rows) {
        Field elementField = new Field("element",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        Field listField = new Field("embedding", FieldType.nullable(new ArrowType.List()), List.of(elementField));
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(listField)), allocator);
        root.allocateNew();

        ListVector listVector = (ListVector) root.getVector("embedding");
        UnionListWriter writer = listVector.getWriter();
        for (int i = 0; i < rows.length; i++) {
            writer.setPosition(i);
            writer.startList();
            for (float v : rows[i]) {
                writer.float4().writeFloat4(v);
            }
            writer.endList();
        }
        writer.setValueCount(rows.length);
        root.setRowCount(rows.length);
        return root;
    }

    @Test
    void writeThenRead_floatListColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        File file = new File(tempDir, "embeddings.parquet");
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildFloatListBatch(allocator,
                     new float[][] {{0.1f, 0.2f, 0.3f}, {0.4f, 0.5f}})) {

            new ArrowToParquetWriter().write(file, batch);

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                assertEquals(2, result.getRowCount());
                ListVector embedding = (ListVector) result.getVector("embedding");

                List<?> row0 = (List<?>) embedding.getObject(0);
                assertEquals(3, row0.size());
                assertEquals(0.1f, ((Number) row0.get(0)).floatValue(), 0.0001f);
                assertEquals(0.3f, ((Number) row0.get(2)).floatValue(), 0.0001f);

                List<?> row1 = (List<?>) embedding.getObject(1);
                assertEquals(2, row1.size());
                assertEquals(0.5f, ((Number) row1.get(1)).floatValue(), 0.0001f);
            }
        }
    }

    @Test
    void writeThenRead_emptyListElement_roundTripsAsEmptyNotNull(@TempDir File tempDir) throws Exception {
        File file = new File(tempDir, "empty-list.parquet");
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildFloatListBatch(allocator, new float[][] {{}, {1.0f}})) {

            new ArrowToParquetWriter().write(file, batch);

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                ListVector embedding = (ListVector) result.getVector("embedding");
                assertFalse(embedding.isNull(0), "an empty list is not the same as a null list");
                assertEquals(0, ((List<?>) embedding.getObject(0)).size());
                assertEquals(1, ((List<?>) embedding.getObject(1)).size());
            }
        }
    }

    @Test
    void writeThenRead_stringListColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        Field elementField = new Field("element", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field listField = new Field("tags", FieldType.nullable(new ArrowType.List()), List.of(elementField));
        File file = new File(tempDir, "tags.parquet");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(listField)), allocator);
            batch.allocateNew();
            ListVector tags = (ListVector) batch.getVector("tags");
            UnionListWriter writer = tags.getWriter();
            writer.setPosition(0);
            writer.startList();
            writeVarChar(writer, tags, "alpha");
            writeVarChar(writer, tags, "beta");
            writer.endList();
            writer.setValueCount(1);
            batch.setRowCount(1);

            new ArrowToParquetWriter().write(file, batch);
            batch.close();

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                ListVector resultTags = (ListVector) result.getVector("tags");
                List<?> row0 = (List<?>) resultTags.getObject(0);
                assertEquals(2, row0.size());
                assertEquals("alpha", row0.get(0).toString());
                assertEquals("beta", row0.get(1).toString());
            }
        }
    }

    private void writeVarChar(UnionListWriter writer, ListVector vector, String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = vector.getAllocator().buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar().writeVarChar(0, bytes.length, buf);
        }
    }
}
