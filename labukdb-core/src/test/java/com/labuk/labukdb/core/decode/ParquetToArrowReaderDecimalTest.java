package com.labuk.labukdb.core.decode;

import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves DECIMAL support against real, hand-built Parquet files — not
 * just round-tripping this project's own writer — covering all four
 * physical representations the Parquet spec legally allows (INT32,
 * INT64, FIXED_LEN_BYTE_ARRAY, BINARY), since a real file from another
 * writer (Spark, etc.) picks its physical type based on precision, not
 * this project's own fixed 16-byte convention.
 */
class ParquetToArrowReaderDecimalTest {

    // DECIMAL(p,s) schema-string annotation syntax, same MessageTypeParser
    // approach NestedTypeDecoderTest already uses for a hand-built fixture.
    private static final String SCHEMA_STRING = """
            message decimals {
              optional int32 small_price (DECIMAL(5,2));
              optional int64 medium_price (DECIMAL(12,2));
              optional fixed_len_byte_array(9) big_price (DECIMAL(20,4));
              optional binary binary_price (DECIMAL(10,2));
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

    private byte[] fixedLenBytes(BigDecimal value, int byteLength) {
        byte[] raw = value.unscaledValue().toByteArray();
        byte[] padded = new byte[byteLength];
        byte signByte = raw.length > 0 && raw[0] < 0 ? (byte) 0xFF : 0x00;
        java.util.Arrays.fill(padded, signByte);
        System.arraycopy(raw, 0, padded, byteLength - raw.length, raw.length);
        return padded;
    }

    private File writeFile(Path dir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STRING);
        File file = dir.resolve("decimals.snappy.parquet").toFile();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);

            // Row 0: positive values, including a column with fewer digits
            // than its byte width to prove padding/sign-extension is right.
            Group row0 = factory.newGroup();
            row0.append("small_price", 1050); // unscaled 1050 -> 10.50
            row0.append("medium_price", 123456789L); // -> 1234567.89
            row0.append("big_price", Binary.fromConstantByteArray(fixedLenBytes(new BigDecimal("12345.6789").movePointRight(4), 9)));
            row0.append("binary_price", Binary.fromConstantByteArray(new BigDecimal("99.99").unscaledValue().toByteArray()));
            writer.write(row0);

            // Row 1: negative values -- proves sign handling across every
            // physical representation, not just the positive case.
            Group row1 = factory.newGroup();
            row1.append("small_price", -500); // -> -5.00
            row1.append("medium_price", -100L); // -> -1.00
            row1.append("big_price", Binary.fromConstantByteArray(fixedLenBytes(new BigDecimal("-1.0000").movePointRight(4), 9)));
            row1.append("binary_price", Binary.fromConstantByteArray(new BigDecimal("-50.00").unscaledValue().toByteArray()));
            writer.write(row1);
        }
        return file;
    }

    @Test
    void readsAllFourPhysicalDecimalEncodings_correctly(@TempDir Path tempDir) throws Exception {
        File file = writeFile(tempDir);

        try (VectorSchemaRoot root = new ParquetToArrowReader().read(file, allocator, null, null, Map.of())) {
            assertEquals(2, root.getRowCount());

            DecimalVector smallPrice = (DecimalVector) root.getVector("small_price");
            assertEquals(new BigDecimal("10.50"), smallPrice.getObject(0));
            assertEquals(new BigDecimal("-5.00"), smallPrice.getObject(1));

            DecimalVector mediumPrice = (DecimalVector) root.getVector("medium_price");
            assertEquals(new BigDecimal("1234567.89"), mediumPrice.getObject(0));
            assertEquals(new BigDecimal("-1.00"), mediumPrice.getObject(1));

            DecimalVector bigPrice = (DecimalVector) root.getVector("big_price");
            assertEquals(new BigDecimal("12345.6789"), bigPrice.getObject(0));
            assertEquals(new BigDecimal("-1.0000"), bigPrice.getObject(1));

            DecimalVector binaryPrice = (DecimalVector) root.getVector("binary_price");
            assertEquals(new BigDecimal("99.99"), binaryPrice.getObject(0));
            assertEquals(new BigDecimal("-50.00"), binaryPrice.getObject(1));
        }
    }

    @Test
    void residualFilter_onDecimalColumn_filtersCorrectly(@TempDir Path tempDir) throws Exception {
        File file = writeFile(tempDir);

        Predicate predicate = Predicate.compare("small_price", ComparisonOp.GT, 0.0);
        try (VectorSchemaRoot root = new ParquetToArrowReader()
                .read(file, allocator, List.of("small_price"), predicate, Map.of())) {
            assertEquals(1, root.getRowCount(), "only the positive 10.50 row should survive small_price > 0.0");
            DecimalVector smallPrice = (DecimalVector) root.getVector("small_price");
            assertEquals(new BigDecimal("10.50"), smallPrice.getObject(0));
        }
    }

    @Test
    void rowGroupStats_onDecimalColumn_pruneCorrectly(@TempDir Path tempDir) throws Exception {
        File file = writeFile(tempDir);
        // small_price's real range in this file is [-5.00, 10.50] -- a
        // predicate asking for > 100 must be prunable (stats say no row
        // could match), one asking for > 0 must not be (10.50 could match).
        // Exercises RowGroupPruner.toJavaValue's decimal branch directly,
        // the actual fix found during research (an INT32-backed decimal
        // stat was previously returned as the raw *unscaled* integer).
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.getAbsolutePath()), new Configuration()))) {
            List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();

            Predicate farOutsideRange = Predicate.compare("small_price", ComparisonOp.GT, 100.0);
            boolean[] prunedMask = RowGroupPruner.pruneMask(blocks, farOutsideRange);
            assertFalse(prunedMask[0], "stats max (10.50) can't satisfy > 100 -- must be pruned");

            Predicate withinRange = Predicate.compare("small_price", ComparisonOp.GT, 0.0);
            boolean[] keptMask = RowGroupPruner.pruneMask(blocks, withinRange);
            assertTrue(keptMask[0], "stats max (10.50) could satisfy > 0 -- must be kept");
        }
    }
}
