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
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
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
import org.apache.parquet.schema.Type;
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
 * <b>{@code LIST}/{@code MAP}/{@code STRUCT}, any depth of nesting</b> —
 * the write-side mirror of {@code decode.NestedTypeDecoder}: the same four
 * leaf primitive kinds (float, double, int32, int64, string) that decoder
 * supports, and the identical 3-level encoding conventions it reads back —
 * {@code LIST} ({@code optional group <name> (LIST) { repeated group list
 * { optional <elem> element; } } }) and {@code MAP} ({@code optional group
 * <name> (MAP) { repeated group key_value { required <keyType> key;
 * optional <valueType> value; } } }, confirmed via {@code javap} on
 * {@code parquet-column-1.13.1.jar}'s {@code ConversionPatterns} — {@code
 * key} is always {@code required}) — built directly via raw {@link
 * GroupType}/{@link OriginalType} rather than {@code Types.list(...)}/
 * {@code Types.map(...)}'s newer {@code LogicalTypeAnnotation}-based
 * builders, so the written file matches exactly what {@code
 * NestedTypeDecoder}'s own shape checks look for. A plain {@code STRUCT}
 * has no {@link OriginalType} annotation at all — just a group whose direct
 * children are the struct's own named fields. List elements, map values,
 * and struct fields are always written {@code optional} (nullable)
 * regardless of the Arrow child field's own nullability — a null value is
 * represented by omitting that field entirely, the same convention {@code
 * NestedTypeDecoder} already reads back via {@code getFieldRepetitionCount
 * == 0}. Map keys are always {@code required} (matches Arrow's own
 * {@code MapVector} validation). Deliberately not wired for decimal/
 * timestamp/boolean leaves inside nested positions — an orthogonal
 * expansion beyond the shape-support this class adds.
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

        if (type instanceof ArrowType.Timestamp) {
            group.append(name, ((TimeStampMicroVector) vector).get(row));
        } else if (type instanceof ArrowType.Decimal decimalType) {
            BigDecimal value = ((DecimalVector) vector).getObject(row);
            byte[] bytes = DecimalCodec.toFixedLengthBytes(value, decimalType.getScale());
            group.append(name, Binary.fromConstantByteArray(bytes));
        } else if (type instanceof ArrowType.List) {
            appendListCell(group.addGroup(name), (ListVector) vector, row);
        } else if (type instanceof ArrowType.Map) {
            appendMapCell(group.addGroup(name), (MapVector) vector, row);
        } else if (type instanceof ArrowType.Struct) {
            appendStructCell(group.addGroup(name), (StructVector) vector, row);
        } else {
            appendPrimitiveValue(group, name, type, vector, row);
        }
    }

    /**
     * Shared by the top-level dispatch and every nested recursive call
     * site — a plain Arrow primitive at one vector index, written into
     * {@code group} under {@code name}. Nested positions reuse this
     * exact switch rather than duplicating it (list elements and map
     * values use it under the fixed names {@code "element"}/{@code
     * "value"}, struct fields under their own real name).
     */
    private void appendPrimitiveValue(Group group, String name, ArrowType type, FieldVector vector, int index) {
        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                group.append(name, ((IntVector) vector).get(index));
            } else if (intType.getBitWidth() == 64) {
                group.append(name, ((BigIntVector) vector).get(index));
            } else {
                throw unsupported(name, type);
            }
        } else if (type instanceof ArrowType.FloatingPoint fp) {
            switch (fp.getPrecision()) {
                case SINGLE -> group.append(name, ((Float4Vector) vector).get(index));
                case DOUBLE -> group.append(name, ((Float8Vector) vector).get(index));
                default -> throw unsupported(name, type);
            }
        } else if (type instanceof ArrowType.Bool) {
            group.append(name, ((BitVector) vector).get(index) != 0);
        } else if (type instanceof ArrowType.Utf8) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            group.append(name, new String(bytes, StandardCharsets.UTF_8));
        } else {
            throw unsupported(name, type);
        }
    }

    /**
     * {@code .addGroup("list")} once per element — exactly the pattern
     * {@code NestedTypeDecoderTest}'s own hand-rolled write fixture already
     * uses, since repeated fields are added by calling {@code addGroup}
     * with the same field name once per occurrence. The element itself may
     * be a nested primitive, or (recursively) another list, a map, or a
     * struct.
     */
    private void appendListCell(Group listGroup, ListVector vector, int row) {
        FieldVector dataVector = vector.getDataVector();
        int start = vector.getElementStartIndex(row);
        int end = vector.getElementEndIndex(row);
        for (int i = start; i < end; i++) {
            Group entry = listGroup.addGroup("list");
            if (!dataVector.isNull(i)) {
                appendNestedValue(entry, "element", dataVector, i);
            }
            // null element -> omit "element" entirely, matches NestedTypeDecoder's read convention
        }
    }

    /**
     * Structurally identical to {@code LIST<Struct<key,value>>} — {@code
     * MapVector} extends {@code ListVector}, so the same element range
     * applies to its {@code "entries"} data vector, whose {@code "key"}/
     * {@code "value"} children are read in lockstep over that range.
     */
    private void appendMapCell(Group mapGroup, MapVector vector, int row) {
        StructVector entries = (StructVector) vector.getDataVector();
        FieldVector keyVector = entries.getChild("key");
        FieldVector valueVector = entries.getChild("value");
        int start = vector.getElementStartIndex(row);
        int end = vector.getElementEndIndex(row);
        for (int i = start; i < end; i++) {
            Group entry = mapGroup.addGroup("key_value");
            appendNestedValue(entry, "key", keyVector, i); // key is always required/non-null
            if (!valueVector.isNull(i)) {
                appendNestedValue(entry, "value", valueVector, i);
            }
        }
    }

    /**
     * A struct doesn't change cardinality per row -- its children live at
     * the same index as the parent, one child {@code Group} field per
     * named Arrow child field.
     */
    private void appendStructCell(Group structGroup, StructVector vector, int row) {
        for (Field childField : vector.getField().getChildren()) {
            String childName = childField.getName();
            FieldVector childVector = vector.getChild(childName);
            if (!childVector.isNull(row)) {
                appendNestedValue(structGroup, childName, childVector, row);
            }
            // null field -> omit entirely, same convention as list/map
        }
    }

    /**
     * Writes one nested value (a list element, a map key/value, or a
     * struct field) at {@code index} of {@code vector} into {@code group}
     * under {@code fieldName} -- dispatches on the Arrow type, recursing
     * through {@link #appendListCell}/{@link #appendMapCell}/{@link
     * #appendStructCell} for nested complex values, or {@link
     * #appendPrimitiveValue} for a leaf.
     */
    private void appendNestedValue(Group group, String fieldName, FieldVector vector, int index) {
        ArrowType type = vector.getField().getType();
        if (type instanceof ArrowType.List) {
            appendListCell(group.addGroup(fieldName), (ListVector) vector, index);
        } else if (type instanceof ArrowType.Map) {
            appendMapCell(group.addGroup(fieldName), (MapVector) vector, index);
        } else if (type instanceof ArrowType.Struct) {
            appendStructCell(group.addGroup(fieldName), (StructVector) vector, index);
        } else {
            appendPrimitiveValue(group, fieldName, type, vector, index);
        }
    }

    private MessageType toParquetSchema(VectorSchemaRoot batch) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field field : batch.getSchema().getFields()) {
            builder.addField(toParquetType(field.getName(), field, Repetition.OPTIONAL));
        }
        return builder.named("labukdb_write");
    }

    /**
     * One recursive schema-building function covering every Arrow type this
     * writer supports -- primitives (unchanged from before) plus List/Map/
     * Struct, each built via raw {@link GroupType}/{@link OriginalType}
     * construction rather than the fluent {@code Types.list(...)}/{@code
     * Types.map(...)} builders, so the written file matches exactly what
     * {@code NestedTypeDecoder}'s own shape checks look for (see this
     * class's javadoc). {@code name}/{@code repetition} are passed in
     * explicitly rather than always taken from {@code field} itself, since
     * a nested position (list element, map key/value) needs a fixed
     * conventional name/repetition regardless of what the Arrow child
     * field happens to be called.
     */
    private Type toParquetType(String name, Field field, Repetition repetition) {
        ArrowType type = field.getType();

        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return Types.primitive(PrimitiveTypeName.INT32, repetition).named(name);
            } else if (intType.getBitWidth() == 64) {
                return Types.primitive(PrimitiveTypeName.INT64, repetition).named(name);
            }
            throw unsupported(name, type);
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
                case DOUBLE -> Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
                default -> throw unsupported(name, type);
            };
        }
        if (type instanceof ArrowType.Bool) {
            return Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
        }
        if (type instanceof ArrowType.Utf8) {
            return Types.primitive(PrimitiveTypeName.BINARY, repetition)
                    .as(LogicalTypeAnnotation.stringType()).named(name);
        }
        if (type instanceof ArrowType.Timestamp) {
            return Types.primitive(PrimitiveTypeName.INT64, repetition)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named(name);
        }
        if (type instanceof ArrowType.Decimal decimalType) {
            DecimalCodec.checkPrecisionSupported(decimalType.getPrecision());
            return Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                    .length(DecimalCodec.FIXED_LEN_BYTES)
                    .as(LogicalTypeAnnotation.decimalType(decimalType.getScale(), decimalType.getPrecision()))
                    .named(name);
        }
        if (type instanceof ArrowType.List) {
            Field elementField = field.getChildren().get(0);
            Type elementType = toParquetType("element", elementField, Repetition.OPTIONAL);
            GroupType repeated = new GroupType(Repetition.REPEATED, "list", elementType);
            return new GroupType(repetition, name, OriginalType.LIST, repeated);
        }
        if (type instanceof ArrowType.Map) {
            Field entriesField = field.getChildren().get(0);
            Field keyField = entriesField.getChildren().get(0);
            Field valueField = entriesField.getChildren().get(1);
            Type keyType = toParquetType("key", keyField, Repetition.REQUIRED);
            Type valueType = toParquetType("value", valueField, Repetition.OPTIONAL);
            GroupType keyValue = new GroupType(Repetition.REPEATED, "key_value", keyType, valueType);
            return new GroupType(repetition, name, OriginalType.MAP, keyValue);
        }
        if (type instanceof ArrowType.Struct) {
            java.util.List<Type> children = new java.util.ArrayList<>();
            for (Field childField : field.getChildren()) {
                children.add(toParquetType(childField.getName(), childField, Repetition.OPTIONAL));
            }
            return new GroupType(repetition, name, children);
        }
        throw unsupported(name, type);
    }

    private static UnsupportedOperationException unsupported(String column, ArrowType type) {
        return new UnsupportedOperationException(
                "Arrow type not supported for encoding yet, column '" + column + "': " + type);
    }
}
