package com.labuk.labukdb.core.encode;

import com.labuk.labukdb.core.decode.DecimalCodec;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Encodes an Arrow {@link VectorSchemaRoot} to a real Parquet file — the
 * write-side mirror of {@code decode/ParquetToArrowReader}, built on the
 * same {@code ExampleParquetWriter}/{@code Group}/{@code SimpleGroupFactory}
 * API already proven in {@code FlightIntegrationTest}'s own fixture-building
 * code, not a new dependency or pattern.
 *
 * <b>Scope, stated plainly:</b> the same flat-primitive scope as the read
 * side's {@code ArrowSchemaMapper} minus INT96 plus two additions —
 * INT32, INT64, FLOAT, DOUBLE, BOOLEAN, UTF8 (BINARY), a modern
 * {@code TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)} logical type for
 * Arrow {@code Timestamp} columns, and DECIMAL (Decimal128 only,
 * precision ≤38 — see {@link DecimalCodec}), always written as a
 * constant 16-byte {@code FIXED_LEN_BYTE_ARRAY} regardless of actual
 * precision (one simple encoding path, not the minimal-bytes-for-precision
 * optimization real writers like Spark use — see {@code DecimalCodec}'s
 * own javadoc for why). Deliberately <b>not</b> INT96 — that's
 * Spark's legacy encoding, which this engine only ever needed to decode
 * for compatibility, never a format a new writer should intentionally
 * produce.
 *
 * <b>{@code LIST<primitive>}</b> — the write-side mirror of {@code
 * decode.NestedTypeDecoder}, same five element types (float, double,
 * int32, int64, string) and the identical 3-level encoding convention
 * ({@code optional group <name> (LIST) { repeated group list { optional
 * <elem> element; } } }) that decoder already reads — built directly via
 * raw {@link GroupType}/{@link OriginalType} rather than {@code
 * Types.list(...)}'s newer {@code LogicalTypeAnnotation}-based builder,
 * so the written file matches exactly what {@code NestedTypeDecoder
 * .isListType} checks for ({@code OriginalType.LIST}), the same thing
 * {@code MessageTypeParser} produces for the old-style {@code (LIST)}
 * schema-string annotation {@code NestedTypeDecoderTest}'s own fixture
 * already uses. List elements are always written {@code optional}
 * (nullable) regardless of the Arrow child field's own nullability — a
 * null element is represented by omitting that repeated group's {@code
 * element} field entirely, the same convention {@code NestedTypeDecoder
 * #appendList} already reads back via {@code getFieldRepetitionCount
 * == 0}. Not yet supported: nested LIST (list of lists), MAP, STRUCT.
 *
 * Every other field is written {@code optional} (nullable), regardless of the
 * Arrow field's own nullability, so a null value can always be represented
 * by simply not appending that field to the row's {@link Group} — the
 * same convention {@code RowAppender} already reads back correctly via
 * {@code getFieldRepetitionCount(column) == 0}.
 */
public final class ArrowToParquetWriter {

    public void write(File outputFile, VectorSchemaRoot batch) throws IOException {
        MessageType schema = toParquetSchema(batch);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(outputFile.getAbsolutePath());

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(hadoopPath).withConf(conf);
        writeWith(builder, schema, batch);
    }

    /**
     * Dispatches on the location — local reuses {@link #write(File, VectorSchemaRoot)}'s
     * exact path; remote builds a {@link HadoopOutputFile} explicitly from
     * {@code location}'s own {@code Configuration} (endpoint/credentials)
     * rather than trusting an implicit default — same reasoning
     * {@code ParquetInputFiles.remote()} already uses on the read side.
     * Confirmed via {@code javap} on {@code parquet-hadoop} that
     * {@code ExampleParquetWriter.builder(OutputFile)} exists for this.
     */
    public void write(TableLocation location, VectorSchemaRoot batch) throws IOException {
        if (!location.isRemote()) {
            write(location.localPath().toFile(), batch);
            return;
        }
        MessageType schema = toParquetSchema(batch);
        Configuration conf = location.hadoopConf();
        GroupWriteSupport.setSchema(schema, conf);
        HadoopOutputFile outputFile = HadoopOutputFile.fromPath(location.hadoopPath(), conf);

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputFile).withConf(conf);
        writeWith(builder, schema, batch);
    }

    private void writeWith(ExampleParquetWriter.Builder builder, MessageType schema, VectorSchemaRoot batch) throws IOException {
        try (ParquetWriter<Group> writer = builder
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            int rowCount = batch.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                Group group = factory.newGroup();
                for (FieldVector vector : batch.getFieldVectors()) {
                    appendCell(group, vector, row);
                }
                writer.write(group);
            }
        }
    }

    private void appendCell(Group group, FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return;
        }
        String name = vector.getField().getName();
        ArrowType type = vector.getField().getType();

        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                group.append(name, ((IntVector) vector).get(row));
            } else if (intType.getBitWidth() == 64) {
                group.append(name, ((BigIntVector) vector).get(row));
            } else {
                throw unsupported(name, type);
            }
        } else if (type instanceof ArrowType.FloatingPoint fp) {
            switch (fp.getPrecision()) {
                case SINGLE -> group.append(name, ((Float4Vector) vector).get(row));
                case DOUBLE -> group.append(name, ((Float8Vector) vector).get(row));
                default -> throw unsupported(name, type);
            }
        } else if (type instanceof ArrowType.Bool) {
            group.append(name, ((BitVector) vector).get(row) != 0);
        } else if (type instanceof ArrowType.Utf8) {
            byte[] bytes = ((VarCharVector) vector).get(row);
            group.append(name, new String(bytes, StandardCharsets.UTF_8));
        } else if (type instanceof ArrowType.Timestamp) {
            group.append(name, ((TimeStampMicroVector) vector).get(row));
        } else if (type instanceof ArrowType.Decimal decimalType) {
            BigDecimal value = ((DecimalVector) vector).getObject(row);
            byte[] bytes = DecimalCodec.toFixedLengthBytes(value, decimalType.getScale());
            group.append(name, Binary.fromConstantByteArray(bytes));
        } else if (type instanceof ArrowType.List) {
            appendListCell(group, name, (ListVector) vector, row);
        } else {
            throw unsupported(name, type);
        }
    }

    /**
     * {@code group.addGroup(name)} once for the outer LIST-annotated group,
     * then {@code .addGroup("list")} once per element — exactly the pattern
     * {@code NestedTypeDecoderTest}'s own hand-rolled write fixture already
     * uses, since repeated fields are added by calling {@code addGroup}
     * with the same field name once per occurrence.
     */
    private void appendListCell(Group group, String name, ListVector vector, int row) {
        Group listGroup = group.addGroup(name);
        FieldVector dataVector = vector.getDataVector();
        ArrowType elementType = dataVector.getField().getType();
        int start = vector.getElementStartIndex(row);
        int end = vector.getElementEndIndex(row);
        for (int i = start; i < end; i++) {
            Group entry = listGroup.addGroup("list");
            if (!dataVector.isNull(i)) {
                appendListElement(entry, elementType, dataVector, i);
            }
            // null element -> omit "element" entirely, matches NestedTypeDecoder#appendList's read convention
        }
    }

    private void appendListElement(Group entry, ArrowType elementType, FieldVector dataVector, int index) {
        if (elementType instanceof ArrowType.FloatingPoint fp) {
            switch (fp.getPrecision()) {
                case SINGLE -> entry.append("element", ((Float4Vector) dataVector).get(index));
                case DOUBLE -> entry.append("element", ((Float8Vector) dataVector).get(index));
                default -> throw unsupportedListElement(elementType);
            }
        } else if (elementType instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                entry.append("element", ((IntVector) dataVector).get(index));
            } else if (intType.getBitWidth() == 64) {
                entry.append("element", ((BigIntVector) dataVector).get(index));
            } else {
                throw unsupportedListElement(elementType);
            }
        } else if (elementType instanceof ArrowType.Utf8) {
            byte[] bytes = ((VarCharVector) dataVector).get(index);
            entry.append("element", new String(bytes, StandardCharsets.UTF_8));
        } else {
            throw unsupportedListElement(elementType);
        }
    }

    private MessageType toParquetSchema(VectorSchemaRoot batch) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field field : batch.getSchema().getFields()) {
            String name = field.getName();
            ArrowType type = field.getType();

            if (type instanceof ArrowType.Int intType) {
                if (intType.getBitWidth() == 32) {
                    builder.optional(PrimitiveTypeName.INT32).named(name);
                } else if (intType.getBitWidth() == 64) {
                    builder.optional(PrimitiveTypeName.INT64).named(name);
                } else {
                    throw unsupported(name, type);
                }
            } else if (type instanceof ArrowType.FloatingPoint fp) {
                switch (fp.getPrecision()) {
                    case SINGLE -> builder.optional(PrimitiveTypeName.FLOAT).named(name);
                    case DOUBLE -> builder.optional(PrimitiveTypeName.DOUBLE).named(name);
                    default -> throw unsupported(name, type);
                }
            } else if (type instanceof ArrowType.Bool) {
                builder.optional(PrimitiveTypeName.BOOLEAN).named(name);
            } else if (type instanceof ArrowType.Utf8) {
                builder.optional(PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(name);
            } else if (type instanceof ArrowType.Timestamp) {
                builder.optional(PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(name);
            } else if (type instanceof ArrowType.Decimal decimalType) {
                DecimalCodec.checkPrecisionSupported(decimalType.getPrecision());
                builder.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(DecimalCodec.FIXED_LEN_BYTES)
                        .as(LogicalTypeAnnotation.decimalType(decimalType.getScale(), decimalType.getPrecision()))
                        .named(name);
            } else if (type instanceof ArrowType.List) {
                builder.addField(buildListType(name, field));
            } else {
                throw unsupported(name, type);
            }
        }
        return builder.named("labukdb_write");
    }

    /**
     * Raw {@link GroupType}/{@link OriginalType} construction, deliberately
     * not {@code Types.list(...)}'s fluent builder — see this class's own
     * javadoc for why matching {@code OriginalType.LIST} exactly matters.
     */
    private GroupType buildListType(String name, Field field) {
        Field elementField = field.getChildren().get(0);
        PrimitiveType elementType = toListElementParquetType(elementField);
        GroupType repeated = new GroupType(Repetition.REPEATED, "list", elementType);
        return new GroupType(Repetition.OPTIONAL, name, OriginalType.LIST, repeated);
    }

    private PrimitiveType toListElementParquetType(Field elementField) {
        ArrowType elementType = elementField.getType();
        if (elementType instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> Types.optional(PrimitiveTypeName.FLOAT).named("element");
                case DOUBLE -> Types.optional(PrimitiveTypeName.DOUBLE).named("element");
                default -> throw unsupportedListElement(elementType);
            };
        }
        if (elementType instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return Types.optional(PrimitiveTypeName.INT32).named("element");
            } else if (intType.getBitWidth() == 64) {
                return Types.optional(PrimitiveTypeName.INT64).named("element");
            }
            throw unsupportedListElement(elementType);
        }
        if (elementType instanceof ArrowType.Utf8) {
            return Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("element");
        }
        throw unsupportedListElement(elementType);
    }

    private static UnsupportedOperationException unsupported(String column, ArrowType type) {
        return new UnsupportedOperationException(
                "Arrow type not supported for encoding yet, column '" + column + "': " + type);
    }

    private static UnsupportedOperationException unsupportedListElement(ArrowType elementType) {
        return new UnsupportedOperationException(
                "List element type not supported for encoding yet: " + elementType);
    }
}
