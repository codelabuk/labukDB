package com.labuk.labukdb.core.encode;

import com.labuk.labukdb.core.decode.DecimalCodec;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
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

    private void writeVarCharNoName(BaseWriter.ListWriter writer, BufferAllocator allocator, String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar().writeVarChar(0, bytes.length, buf);
        }
    }

    private void writeVarCharNamed(NullableStructWriter writer, BufferAllocator allocator, String fieldName, String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar(fieldName).writeVarChar(0, bytes.length, buf);
        }
    }

    /**
     * {@code LIST<LIST<int>>} -- proves list nesting of any depth needs no
     * new Parquet-side machinery, just the recursive writer dispatch (see
     * {@code ArrowToParquetWriter}/{@code NestedTypeDecoder}'s own javadoc).
     */
    @Test
    void writeThenRead_listOfListColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        Field grandchildField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field innerListField = new Field("element", FieldType.nullable(new ArrowType.List()), List.of(grandchildField));
        Field outerListField = new Field("matrix", FieldType.nullable(new ArrowType.List()), List.of(innerListField));
        File file = new File(tempDir, "matrix.parquet");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(outerListField)), allocator);
            batch.allocateNew();
            ListVector matrix = (ListVector) batch.getVector("matrix");
            UnionListWriter writer = matrix.getWriter();

            // Row 0: [[1,2,3],[4,5]] -- two inner lists of different lengths.
            // Row 1: [[6]] -- a single inner list.
            int[][][] rows = {{{1, 2, 3}, {4, 5}}, {{6}}};
            for (int row = 0; row < rows.length; row++) {
                writer.setPosition(row);
                writer.startList();
                for (int[] innerList : rows[row]) {
                    BaseWriter.ListWriter innerWriter = writer.list();
                    innerWriter.startList();
                    for (int value : innerList) {
                        innerWriter.integer().writeInt(value);
                    }
                    innerWriter.endList();
                }
                writer.endList();
            }
            writer.setValueCount(rows.length);
            batch.setRowCount(rows.length);

            new ArrowToParquetWriter().write(file, batch);
            batch.close();

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                ListVector resultMatrix = (ListVector) result.getVector("matrix");
                List<?> row0 = (List<?>) resultMatrix.getObject(0);
                assertEquals(2, row0.size(), "row 0 should have two inner lists");
                assertEquals(3, ((List<?>) row0.get(0)).size());
                assertEquals(1, ((Number) ((List<?>) row0.get(0)).get(0)).intValue());
                assertEquals(3, ((Number) ((List<?>) row0.get(0)).get(2)).intValue());
                assertEquals(2, ((List<?>) row0.get(1)).size());
                assertEquals(5, ((Number) ((List<?>) row0.get(1)).get(1)).intValue());

                List<?> row1 = (List<?>) resultMatrix.getObject(1);
                assertEquals(1, row1.size(), "row 1 should have one inner list");
                assertEquals(6, ((Number) ((List<?>) row1.get(0)).get(0)).intValue());
            }
        }
    }

    /** {@code STRUCT<name: string, age: int>}. */
    @Test
    void writeThenRead_structColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        Field nameField = new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field personField = new Field("person", FieldType.nullable(ArrowType.Struct.INSTANCE),
                List.of(nameField, ageField));
        File file = new File(tempDir, "person.parquet");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(personField)), allocator);
            batch.allocateNew();
            StructVector person = (StructVector) batch.getVector("person");
            NullableStructWriter writer = person.getWriter();

            writer.setPosition(0);
            writer.start();
            writeVarCharNamed(writer, allocator, "name", "Ada");
            writer.integer("age").writeInt(30);
            writer.end();

            writer.setPosition(1);
            writer.start();
            writeVarCharNamed(writer, allocator, "name", "Grace");
            writer.integer("age").writeInt(85);
            writer.end();

            writer.setValueCount(2);
            batch.setRowCount(2);

            new ArrowToParquetWriter().write(file, batch);
            batch.close();

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                StructVector resultPerson = (StructVector) result.getVector("person");
                assertEquals("Ada", ((java.util.Map<?, ?>) resultPerson.getObject(0)).get("name").toString());
                assertEquals(30, ((Number) ((java.util.Map<?, ?>) resultPerson.getObject(0)).get("age")).intValue());
                assertEquals("Grace", ((java.util.Map<?, ?>) resultPerson.getObject(1)).get("name").toString());
            }
        }
    }

    /** {@code MAP<string, int>}. */
    @Test
    void writeThenRead_mapColumn_roundTripsExactly(@TempDir File tempDir) throws Exception {
        Field keyField = new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field valueField = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field entriesField = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                List.of(keyField, valueField));
        Field mapField = new Field("attrs", FieldType.nullable(new ArrowType.Map(false)), List.of(entriesField));
        File file = new File(tempDir, "attrs.parquet");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(mapField)), allocator);
            batch.allocateNew();
            MapVector attrs = (MapVector) batch.getVector("attrs");
            UnionMapWriter writer = attrs.getWriter();

            writer.setPosition(0);
            writer.startMap();
            writer.startEntry();
            writeVarCharNoName(writer.key(), allocator, "height");
            writer.value().integer().writeInt(180);
            writer.endEntry();
            writer.startEntry();
            writeVarCharNoName(writer.key(), allocator, "weight");
            writer.value().integer().writeInt(75);
            writer.endEntry();
            writer.endMap();

            writer.setValueCount(1);
            batch.setRowCount(1);

            new ArrowToParquetWriter().write(file, batch);
            batch.close();

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                MapVector resultAttrs = (MapVector) result.getVector("attrs");
                List<?> entries = (List<?>) resultAttrs.getObject(0);
                assertEquals(2, entries.size());
                java.util.Map<?, ?> firstEntry = (java.util.Map<?, ?>) entries.get(0);
                assertEquals("height", firstEntry.get("key").toString());
                assertEquals(180, ((Number) firstEntry.get("value")).intValue());
            }
        }
    }

    /** {@code STRUCT<tags: LIST<string>>} -- proves a struct field can itself be a nested complex type. */
    @Test
    void writeThenRead_structContainingListField_roundTripsExactly(@TempDir File tempDir) throws Exception {
        Field elementField = new Field("element", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field tagsField = new Field("tags", FieldType.nullable(new ArrowType.List()), List.of(elementField));
        Field profileField = new Field("profile", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(tagsField));
        File file = new File(tempDir, "profile.parquet");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(profileField)), allocator);
            batch.allocateNew();
            StructVector profile = (StructVector) batch.getVector("profile");
            NullableStructWriter writer = profile.getWriter();

            writer.setPosition(0);
            writer.start();
            BaseWriter.ListWriter tagsWriter = writer.list("tags");
            tagsWriter.startList();
            writeVarCharNoName(tagsWriter, allocator, "alpha");
            writeVarCharNoName(tagsWriter, allocator, "beta");
            tagsWriter.endList();
            writer.end();

            writer.setValueCount(1);
            batch.setRowCount(1);

            new ArrowToParquetWriter().write(file, batch);
            batch.close();

            try (VectorSchemaRoot result = new ParquetToArrowReader().read(file, allocator, null, null)) {
                StructVector resultProfile = (StructVector) result.getVector("profile");
                java.util.Map<?, ?> row0 = (java.util.Map<?, ?>) resultProfile.getObject(0);
                List<?> tags = (List<?>) row0.get("tags");
                assertEquals(2, tags.size());
                assertEquals("alpha", tags.get(0).toString());
            }
        }
    }
}
