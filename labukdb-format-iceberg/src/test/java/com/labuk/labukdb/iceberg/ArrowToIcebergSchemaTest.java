package com.labuk.labukdb.iceberg;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers {@link ArrowToIcebergSchema}'s type mapping directly — every
 * type {@code core.encode.ArrowToParquetWriter} can actually write
 * (deliberately the same set, see that class's own javadoc), plus the
 * one thing a schema builder test must check that a round-trip test
 * can't: that nested-type field IDs really do come out globally unique
 * (Iceberg's own hard requirement), not just "doesn't throw."
 */
class ArrowToIcebergSchemaTest {

    private static Field field(String name, ArrowType type, boolean nullable) {
        return new Field(name, nullable ? FieldType.nullable(type) : FieldType.notNullable(type), null);
    }

    @Test
    void primitiveTypes_mapToExpectedIcebergTypes() {
        Schema arrowSchema = new Schema(List.of(
                field("id", new ArrowType.Int(32, true), false),
                field("count", new ArrowType.Int(64, true), true),
                field("ratio", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true),
                field("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
                field("active", ArrowType.Bool.INSTANCE, true),
                field("name", ArrowType.Utf8.INSTANCE, true),
                field("createdAt", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true),
                field("price", new ArrowType.Decimal(10, 2, 128), true)));

        org.apache.iceberg.Schema result = ArrowToIcebergSchema.toIcebergSchema(arrowSchema);

        assertEquals(Types.IntegerType.get(), result.findType("id"));
        assertFalse(result.findField("id").isOptional(), "non-nullable Arrow field must become a required Iceberg field");
        assertEquals(Types.LongType.get(), result.findType("count"));
        assertEquals(Types.FloatType.get(), result.findType("ratio"));
        assertEquals(Types.DoubleType.get(), result.findType("score"));
        assertEquals(Types.BooleanType.get(), result.findType("active"));
        assertEquals(Types.StringType.get(), result.findType("name"));
        assertEquals(Types.TimestampType.withZone(), result.findType("createdAt"));
        assertEquals(Types.DecimalType.of(10, 2), result.findType("price"));
    }

    @Test
    void listOfFloat_mapsToListTypeWithDistinctFieldIds() {
        Field elementField = field("element", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true);
        Field listField = new Field("embedding", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        org.apache.iceberg.Schema result = ArrowToIcebergSchema.toIcebergSchema(new Schema(List.of(listField)));

        Type embeddingType = result.findType("embedding");
        assertInstanceOf(Types.ListType.class, embeddingType);
        Types.ListType listType = (Types.ListType) embeddingType;
        assertEquals(Types.FloatType.get(), listType.elementType());
        assertTrue(listType.isElementOptional(), "the element field in this test is nullable");

        int outerFieldId = result.findField("embedding").fieldId();
        int elementFieldId = listType.elementId();
        assertNotEquals(outerFieldId, elementFieldId, "the list field and its element must have distinct Iceberg field IDs");
    }

    @Test
    void listOfRequiredString_elementIsRequired() {
        Field elementField = field("element", ArrowType.Utf8.INSTANCE, false);
        Field listField = new Field("tags", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        org.apache.iceberg.Schema result = ArrowToIcebergSchema.toIcebergSchema(new Schema(List.of(listField)));
        Types.ListType listType = (Types.ListType) result.findType("tags");

        assertEquals(Types.StringType.get(), listType.elementType());
        assertFalse(listType.isElementOptional());
    }

    @Test
    void multipleFieldsIncludingList_allGetDistinctFieldIds() {
        Field elementField = field("element", new ArrowType.Int(32, true), true);
        Field listField = new Field("values", FieldType.nullable(new ArrowType.List()), List.of(elementField));
        Schema arrowSchema = new Schema(List.of(
                field("id", new ArrowType.Int(32, true), false),
                listField,
                field("name", ArrowType.Utf8.INSTANCE, true)));

        org.apache.iceberg.Schema result = ArrowToIcebergSchema.toIcebergSchema(arrowSchema);

        java.util.Set<Integer> allIds = new java.util.HashSet<>();
        for (org.apache.iceberg.types.Types.NestedField f : result.columns()) {
            assertTrue(allIds.add(f.fieldId()), "duplicate top-level field ID: " + f.fieldId());
        }
        Types.ListType listType = (Types.ListType) result.findType("values");
        assertTrue(allIds.add(listType.elementId()), "the list element's field ID must not collide with any top-level field ID");
    }

    @Test
    void unsupportedArrowType_throwsRatherThanGuessing() {
        Field unsupportedField = field("blob", new ArrowType.Binary(), true);
        Schema arrowSchema = new Schema(List.of(unsupportedField));

        assertThrows(UnsupportedOperationException.class,
                () -> ArrowToIcebergSchema.toIcebergSchema(arrowSchema));
    }
}
