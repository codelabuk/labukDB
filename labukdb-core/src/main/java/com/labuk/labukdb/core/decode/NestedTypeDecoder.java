package com.labuk.labukdb.core.decode;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes nested Parquet types into Arrow vectors.
 *
 * <b>Scope — what's implemented:</b>
 * <ul>
 *   <li>{@code LIST<primitive>} / {@code LIST<LIST<...>>} / {@code LIST<MAP>} /
 *       {@code LIST<STRUCT>} — any depth of list nesting, mapped to Arrow
 *       {@code ListVector}.</li>
 *   <li>{@code MAP<primitive, T>} — mapped to Arrow {@code MapVector}, which
 *       Arrow itself models as {@code List<Struct<key,value>>}: one
 *       non-nullable {@code "entries"} struct child with {@code "key"}
 *       (always non-nullable) and {@code "value"} (nullable) grandchildren.
 *       {@code T} may itself be any supported nested type.</li>
 *   <li>{@code STRUCT<...>} — mapped to Arrow {@code StructVector}, fields
 *       may themselves be any supported nested type.</li>
 * </ul>
 * The four leaf primitive kinds a nested position may bottom out in are
 * float, double, int32, int64, and string (the same set this class has
 * always supported for {@code LIST<primitive>} elements) — not the full
 * flat-column primitive set ({@link ArrowSchemaMapper} also handles
 * DECIMAL/BOOLEAN/TIMESTAMP/INT96, but wiring those into every nested
 * writer position is a separate, unverified expansion not needed for
 * MAP/STRUCT/nested-LIST support itself).
 *
 * <p>The implementation fails loudly rather than silently producing wrong
 * data for unrecognized structures — notably, a {@code GroupType} is only
 * ever treated as a plain {@code STRUCT} when it carries <b>no</b>
 * {@link OriginalType} annotation at all; an unrecognized annotation (e.g.
 * the legacy {@code MAP_KEY_VALUE}, which this class deliberately does not
 * read — see {@code ArrowToParquetWriter}, which only ever writes the
 * modern {@code OriginalType.MAP} convention) still throws instead of being
 * silently misread as a struct.
 *
 * <b>Why this matters for the lakehouse-gateway use case:</b> embedding
 * vectors are almost always stored as {@code ARRAY<float>} or
 * {@code ARRAY<double>} in Delta tables written by Spark ML or MLflow, and
 * richer document/metadata columns often use {@code STRUCT}/{@code MAP}.
 * Without this class, any table containing such a column would throw at
 * the schema-mapping step.
 */
final class NestedTypeDecoder {

    private NestedTypeDecoder() {
    }

    /**
     * Returns true if this Parquet type is a nested type this decoder handles.
     * Used by {@link ArrowSchemaMapper} and {@link ParquetToArrowReader} to
     * decide whether to route through this class.
     */
    static boolean isSupported(Type parquetType) {
        if (parquetType.isPrimitive()) {
            return false;
        }
        GroupType group = parquetType.asGroupType();
        return isListType(group) || isMapType(group) || isStructType(group);
    }

    /**
     * Maps a supported nested Parquet type to its Arrow equivalent field,
     * recursing into list elements / map key+value / struct fields of any
     * depth. Only call after {@link #isSupported} returns true.
     */
    static Field toArrowField(String columnName, Type parquetType) {
        return toArrowField(columnName, parquetType, true);
    }

    private static Field toArrowField(String name, Type parquetType, boolean nullable) {
        if (parquetType.isPrimitive()) {
            ArrowType arrowType = primitiveToArrowType(parquetType.asPrimitiveType());
            return new Field(name, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
        }

        GroupType group = parquetType.asGroupType();
        if (isListType(group)) {
            Field elementField = toArrowField("element", getListElementType(group), true);
            ArrowType.List listType = new ArrowType.List();
            return new Field(name, nullable ? FieldType.nullable(listType) : FieldType.notNullable(listType),
                    List.of(elementField));
        }
        if (isMapType(group)) {
            GroupType keyValueGroup = group.getFields().get(0).asGroupType();
            Field keyField = toArrowField("key", keyValueGroup.getFields().get(0), false);
            Field valueField = toArrowField("value", keyValueGroup.getFields().get(1), true);
            Field entriesField = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    List.of(keyField, valueField));
            ArrowType.Map mapType = new ArrowType.Map(false);
            return new Field(name, nullable ? FieldType.nullable(mapType) : FieldType.notNullable(mapType),
                    List.of(entriesField));
        }
        if (isStructType(group)) {
            List<Field> children = new ArrayList<>();
            for (Type field : group.getFields()) {
                children.add(toArrowField(field.getName(), field, true));
            }
            return new Field(name, nullable ? FieldType.nullable(ArrowType.Struct.INSTANCE)
                    : FieldType.notNullable(ArrowType.Struct.INSTANCE), children);
        }
        throw new UnsupportedOperationException(
                "Nested Parquet type not supported: " + parquetType + " (column '" + name + "'). "
                        + "Supported: LIST<...>, MAP<...,...>, STRUCT<...>.");
    }

    /**
     * Appends one row's value for a nested-typed column from a decoded
     * Parquet {@code Group} into the Arrow vector at {@code destIndex}.
     * Dispatches on the real vector subtype (checking {@link MapVector}
     * before {@link ListVector}, since Arrow models {@code MapVector} as a
     * {@code ListVector} subclass) and recurses through the matching
     * writer-based helper below.
     */
    static void appendValue(Group row, String columnName, Type parquetType, FieldVector vector, int destIndex) {
        if (row.getFieldRepetitionCount(columnName) == 0) {
            vector.setNull(destIndex);
            return;
        }

        GroupType group = parquetType.asGroupType();
        BufferAllocator allocator = vector.getAllocator();

        if (vector instanceof MapVector mapVector) {
            UnionMapWriter writer = mapVector.getWriter();
            writer.setPosition(destIndex);
            writeMap(writer, row.getGroup(columnName, 0), group, allocator);
        } else if (vector instanceof ListVector listVector) {
            UnionListWriter writer = listVector.getWriter();
            writer.setPosition(destIndex);
            writeList(writer, row.getGroup(columnName, 0), group, allocator);
        } else if (vector instanceof StructVector structVector) {
            NullableStructWriter writer = structVector.getWriter();
            writer.setPosition(destIndex);
            writeStruct(writer, row.getGroup(columnName, 0), group, allocator);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported nested vector type for column '" + columnName + "': " + vector.getClass());
        }
    }

    // --- recursive writer-based append helpers ---

    private static void writeList(BaseWriter.ListWriter writer, Group listGroup, GroupType listAnnotatedGroup,
                                   BufferAllocator allocator) {
        Type elementType = getListElementType(listAnnotatedGroup);
        String listFieldName = listGroup.getType().getFieldName(0);
        int count = listGroup.getFieldRepetitionCount(listFieldName);

        writer.startList();
        for (int i = 0; i < count; i++) {
            Group entry = listGroup.getGroup(listFieldName, i);
            String elementFieldName = entry.getType().getFieldName(0);
            appendListSlot(writer, entry, elementFieldName, elementType, allocator);
        }
        writer.endList();
    }

    private static void writeMap(BaseWriter.MapWriter writer, Group mapGroup, GroupType mapAnnotatedGroup,
                                  BufferAllocator allocator) {
        GroupType keyValueGroup = mapAnnotatedGroup.getFields().get(0).asGroupType();
        Type keyType = keyValueGroup.getFields().get(0);
        Type valueType = keyValueGroup.getFields().get(1);
        String keyValueFieldName = mapGroup.getType().getFieldName(0);
        int count = mapGroup.getFieldRepetitionCount(keyValueFieldName);

        writer.startMap();
        for (int i = 0; i < count; i++) {
            Group entry = mapGroup.getGroup(keyValueFieldName, i);
            writer.startEntry();
            appendListSlot(writer.key(), entry, "key", keyType, allocator);
            appendListSlot(writer.value(), entry, "value", valueType, allocator);
            writer.endEntry();
        }
        writer.endMap();
    }

    private static void writeStruct(BaseWriter.StructWriter writer, Group structGroup, GroupType structType,
                                     BufferAllocator allocator) {
        writer.start();
        for (Type field : structType.getFields()) {
            String fieldName = field.getName();
            if (structGroup.getFieldRepetitionCount(fieldName) == 0) {
                continue; // null field -- omitted, matches the list/map null-element convention
            }
            appendStructField(writer, fieldName, structGroup, field, allocator);
        }
        writer.end();
    }

    /** A "no-name slot" write — a list element, or a map key/value positioned via {@code .key()}/{@code .value()}. */
    private static void appendListSlot(BaseWriter.ListWriter writer, Group parent, String fieldName, Type elementType,
                                        BufferAllocator allocator) {
        if (parent.getFieldRepetitionCount(fieldName) == 0) {
            return; // null element -- omitted, same convention this class has always used
        }
        if (elementType.isPrimitive()) {
            appendPrimitiveToSlot(writer, parent, fieldName, elementType.asPrimitiveType(), allocator);
            return;
        }

        GroupType elementGroup = elementType.asGroupType();
        Group nested = parent.getGroup(fieldName, 0);
        if (isMapType(elementGroup)) {
            writeMap(writer.map(false), nested, elementGroup, allocator);
        } else if (isListType(elementGroup)) {
            writeList(writer.list(), nested, elementGroup, allocator);
        } else if (isStructType(elementGroup)) {
            writeStruct(writer.struct(), nested, elementGroup, allocator);
        } else {
            throw new UnsupportedOperationException("Unsupported nested element type: " + elementType);
        }
    }

    /** A named struct-field write. */
    private static void appendStructField(BaseWriter.StructWriter writer, String fieldName, Group structGroup,
                                           Type fieldType, BufferAllocator allocator) {
        if (fieldType.isPrimitive()) {
            appendPrimitiveNamed(writer, fieldName, structGroup, fieldType.asPrimitiveType(), allocator);
            return;
        }

        GroupType fieldGroup = fieldType.asGroupType();
        Group nested = structGroup.getGroup(fieldName, 0);
        if (isMapType(fieldGroup)) {
            writeMap(writer.map(fieldName, false), nested, fieldGroup, allocator);
        } else if (isListType(fieldGroup)) {
            writeList(writer.list(fieldName), nested, fieldGroup, allocator);
        } else if (isStructType(fieldGroup)) {
            writeStruct(writer.struct(fieldName), nested, fieldGroup, allocator);
        } else {
            throw new UnsupportedOperationException("Unsupported nested struct field type: " + fieldType);
        }
    }

    private static void appendPrimitiveToSlot(BaseWriter.ListWriter writer, Group parent, String fieldName,
                                                PrimitiveType primitiveType, BufferAllocator allocator) {
        switch (primitiveType.getPrimitiveTypeName()) {
            case FLOAT:
                writer.float4().writeFloat4(parent.getFloat(fieldName, 0));
                break;
            case DOUBLE:
                writer.float8().writeFloat8(parent.getDouble(fieldName, 0));
                break;
            case INT32:
                writer.integer().writeInt(parent.getInteger(fieldName, 0));
                break;
            case INT64:
                writer.bigInt().writeBigInt(parent.getLong(fieldName, 0));
                break;
            case BINARY:
                writeVarCharSlot(writer.varChar(), parent.getString(fieldName, 0), allocator);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Nested primitive element type not supported: " + primitiveType.getPrimitiveTypeName());
        }
    }

    private static void appendPrimitiveNamed(BaseWriter.StructWriter writer, String fieldName, Group parent,
                                               PrimitiveType primitiveType, BufferAllocator allocator) {
        switch (primitiveType.getPrimitiveTypeName()) {
            case FLOAT:
                writer.float4(fieldName).writeFloat4(parent.getFloat(fieldName, 0));
                break;
            case DOUBLE:
                writer.float8(fieldName).writeFloat8(parent.getDouble(fieldName, 0));
                break;
            case INT32:
                writer.integer(fieldName).writeInt(parent.getInteger(fieldName, 0));
                break;
            case INT64:
                writer.bigInt(fieldName).writeBigInt(parent.getLong(fieldName, 0));
                break;
            case BINARY:
                writeVarCharSlot(writer.varChar(fieldName), parent.getString(fieldName, 0), allocator);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Nested struct field primitive type not supported: " + primitiveType.getPrimitiveTypeName());
        }
    }

    private static void writeVarCharSlot(VarCharWriter writer, String value, BufferAllocator allocator) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ArrowBuf buf = allocator.buffer(bytes.length);
        buf.writeBytes(bytes);
        writer.writeVarChar(0, bytes.length, buf);
        buf.close();
    }

    // --- shape checks and primitive leaf mapping ---

    private static boolean isListType(GroupType group) {
        // The 3-level encoding convention: outer group (LIST) contains exactly
        // one repeated group ("list" by convention) which contains one field
        // ("element" by convention).
        return group.getOriginalType() == OriginalType.LIST && group.getFieldCount() == 1;
    }

    private static boolean isMapType(GroupType group) {
        // Mirrors isListType: outer group (MAP) contains exactly one repeated
        // group ("key_value" by convention) with "key"/"value" children.
        return group.getOriginalType() == OriginalType.MAP && group.getFieldCount() == 1;
    }

    /**
     * A plain struct is any group with NO {@link OriginalType} annotation at
     * all -- deliberately exact, not "not list and not map", so an
     * unrecognized annotation (e.g. legacy MAP_KEY_VALUE) still fails loudly
     * instead of being silently misread as a struct.
     */
    private static boolean isStructType(GroupType group) {
        return group.getOriginalType() == null;
    }

    private static Type getListElementType(GroupType listGroup) {
        // 3-level: outer(LIST) -> repeated group -> element
        GroupType repeated = listGroup.getFields().get(0).asGroupType();
        return repeated.getFields().get(0);
    }

    private static ArrowType primitiveToArrowType(PrimitiveType type) {
        PrimitiveType.PrimitiveTypeName name = type.getPrimitiveTypeName();
        switch (name) {
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case INT32:
                return new ArrowType.Int(32, true);
            case INT64:
                return new ArrowType.Int(64, true);
            case BINARY:
                return ArrowType.Utf8.INSTANCE;
            default:
                throw new UnsupportedOperationException("Nested primitive leaf type not supported: " + name);
        }
    }
}
