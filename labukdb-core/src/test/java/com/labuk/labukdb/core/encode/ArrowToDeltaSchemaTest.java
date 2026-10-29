package com.labuk.labukdb.core.encode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * First direct test of {@link ArrowToDeltaSchema} — previously only
 * exercised indirectly via {@code TableCreator}'s own tests. Covers the
 * one shape change a {@code List} field requires: Delta's array type is
 * a nested JSON object ({@code {"type":"array","elementType":...,
 * "containsNull":...}}), not a plain type-name string the way every
 * other supported type is — confirmed against the Delta Sharing protocol
 * spec and delta-rs's own schema handling, not assumed.
 */
class ArrowToDeltaSchemaTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void primitiveField_encodesTypeAsPlainString() throws Exception {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        String json = ArrowToDeltaSchema.toSchemaString(new Schema(List.of(idField)));

        JsonNode fieldNode = MAPPER.readTree(json).get("fields").get(0);
        assertTrue(fieldNode.get("type").isTextual(), "a primitive field's type must stay a plain string");
        assertEquals("integer", fieldNode.get("type").asText());
    }

    @Test
    void listOfFloatField_encodesAsArrayObject_notAPlainString() throws Exception {
        Field elementField = new Field("element",
                FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null);
        Field listField = new Field("embedding", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        String json = ArrowToDeltaSchema.toSchemaString(new Schema(List.of(listField)));
        JsonNode fieldNode = MAPPER.readTree(json).get("fields").get(0);
        JsonNode typeNode = fieldNode.get("type");

        assertTrue(typeNode.isObject(), "a List field's type must be a nested object, not a string");
        assertEquals("array", typeNode.get("type").asText());
        assertEquals("float", typeNode.get("elementType").asText());
        assertTrue(typeNode.get("containsNull").asBoolean(), "the element field in this test is nullable");
    }

    @Test
    void listOfStringField_elementTypeIsString() throws Exception {
        Field elementField = new Field("element", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field listField = new Field("tags", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        String json = ArrowToDeltaSchema.toSchemaString(new Schema(List.of(listField)));
        JsonNode typeNode = MAPPER.readTree(json).get("fields").get(0).get("type");

        assertEquals("string", typeNode.get("elementType").asText());
        assertEquals(false, typeNode.get("containsNull").asBoolean(),
                "containsNull must reflect the element field's own nullability, not the list field's");
    }
}
