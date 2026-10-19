package com.labuk.labukdb.core.decode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Decodes nested Parquet types into Arrow vectors.
 *
 * <b>Scope — what's implemented:</b>
 * <ul>
 *   <li>{@code LIST<float>} — the most common embedding vector representation.
 *       Mapped to Arrow {@code ListVector<Float4>}.</li>
 *   <li>{@code LIST<double>} — higher-precision embeddings or numeric arrays.
 *       Mapped to Arrow {@code ListVector<Float8>}.</li>
 *   <li>{@code LIST<int>} / {@code LIST<long>} — integer arrays.
 *       Mapped to Arrow {@code ListVector<Int32/Int64>}.</li>
 *   <li>{@code LIST<string>} — string arrays.
 *       Mapped to Arrow {@code ListVector<Utf8>}.</li>
 * </ul>
 *
 * <b>What's not implemented yet:</b> nested LIST (list of lists), MAP,
 * STRUCT — these are genuinely rare in Delta tables that aren't purpose-built
 * for graph or document data, and none of the known consumer use cases
 * (embedding vectors, tag arrays, multi-value fields) require them.
 * The implementation fails loudly rather than silently producing wrong data
 * for unrecognized structures.
 *
 * <b>Why this matters for the lakehouse-gateway use case:</b> embedding
 * vectors are almost always stored as {@code ARRAY<float>} or
 * {@code ARRAY<double>} in Delta tables written by Spark ML or MLflow.
 * Without this class, any table containing an embedding column would
 * throw at the schema-mapping step — blocking the vector-search and
 * hybrid-retrieval use cases that {@code lakehouse-gateway} is designed for.
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
        return isListType(group);
    }

    /**
     * Maps a supported nested Parquet type to its Arrow equivalent field.
     * Only call after {@link #isSupported} returns true.
     */
    static Field toArrowField(String columnName, Type parquetType) {
        GroupType group = parquetType.asGroupType();
        if (isListType(group)) {
            Type elementType = getListElementType(group);
            ArrowType elementArrowType = primitiveToArrowType(elementType);
            Field elementField = new Field("element",
                    FieldType.nullable(elementArrowType), null);
            return new Field(columnName,
                    FieldType.nullable(new ArrowType.List()),
                    List.of(elementField));
        }
        throw new UnsupportedOperationException(
                "Nested Parquet type not supported: " + parquetType + " (column '" + columnName + "'). "
                        + "Supported nested types: LIST<float>, LIST<double>, LIST<int>, "
                        + "LIST<long>, LIST<string>.");
    }

    /**
     * Creates an Arrow {@link ListVector} for the given column name and allocator.
     */
    static ListVector createListVector(String columnName, Type parquetType,
                                        BufferAllocator allocator) {
        Field field = toArrowField(columnName, parquetType);
        ListVector vector = ListVector.empty(columnName, allocator);
        vector.initializeChildrenFromFields(field.getChildren());
        vector.allocateNew();
        return vector;
    }

    /**
     * Appends one row's list value from a decoded Parquet {@code Group}
     * into the Arrow {@link ListVector} at {@code destIndex}.
     */
    static void appendList(Group row, String columnName, Type parquetType,
                            ListVector vector, int destIndex) {
        if (row.getFieldRepetitionCount(columnName) == 0) {
            vector.setNull(destIndex);
            return;
        }

        GroupType group = parquetType.asGroupType();
        Type elementType = getListElementType(group);
        Group listGroup = row.getGroup(columnName, 0);
        String listFieldName = listGroup.getType().getFieldName(0);
        int elementCount = listGroup.getFieldRepetitionCount(listFieldName);

        UnionListWriter writer = vector.getWriter();
        writer.setPosition(destIndex);
        writer.startList();

        PrimitiveType.PrimitiveTypeName primitiveTypeName =
                elementType.asPrimitiveType().getPrimitiveTypeName();

        for (int i = 0; i < elementCount; i++) {
            Group listEntry = listGroup.getGroup(listFieldName, i);
            String elementFieldName = listEntry.getType().getFieldName(0);
            if (listEntry.getFieldRepetitionCount(elementFieldName) == 0) {
                // null element in the list
                continue;
            }
            switch (primitiveTypeName) {
                case FLOAT:
                    writer.float4().writeFloat4(listEntry.getFloat(elementFieldName, 0));
                    break;
                case DOUBLE:
                    writer.float8().writeFloat8(listEntry.getDouble(elementFieldName, 0));
                    break;
                case INT32:
                    writer.integer().writeInt(listEntry.getInteger(elementFieldName, 0));
                    break;
                case INT64:
                    writer.bigInt().writeBigInt(listEntry.getLong(elementFieldName, 0));
                    break;
                case BINARY:
                    // String list element — write as VarChar bytes
                    byte[] bytes = listEntry.getString(elementFieldName, 0)
                            .getBytes(StandardCharsets.UTF_8);
                    org.apache.arrow.memory.ArrowBuf buf =
                            vector.getAllocator().buffer(bytes.length);
                    buf.writeBytes(bytes);
                    writer.varChar().writeVarChar(0, bytes.length, buf);
                    buf.close();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "List element type not supported: " + primitiveTypeName);
            }
        }

        writer.endList();
    }

    // --- private helpers ---

    private static boolean isListType(GroupType group) {
        // parquet-mr 1.13.1 uses the OriginalType enum for backward-compatible
        // annotations. A LIST group has OriginalType.LIST set, regardless of
        // whether it was written with the old or new annotation API.
        // The 3-level encoding convention: outer group (LIST) contains exactly
        // one repeated group ("list" by convention) which contains one field
        // ("element" by convention).
        return group.getOriginalType() == org.apache.parquet.schema.OriginalType.LIST
                && group.getFieldCount() == 1;
    }

    private static Type getListElementType(GroupType listGroup) {
        // 3-level: outer(LIST) → repeated group → element
        GroupType repeated = listGroup.getFields().get(0).asGroupType();
        return repeated.getFields().get(0);
    }

    private static ArrowType primitiveToArrowType(Type type) {
        if (type.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
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
                    throw new UnsupportedOperationException(
                            "List element primitive type not supported: " + name);
            }
        }
        throw new UnsupportedOperationException("Nested list elements must be primitive: " + type);
    }
}
