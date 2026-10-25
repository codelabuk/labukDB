package com.labuk.labukdb.core.encode;

import com.labuk.labukdb.core.decode.DecimalCodec;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
}
