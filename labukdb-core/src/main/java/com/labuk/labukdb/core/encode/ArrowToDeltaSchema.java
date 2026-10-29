package com.labuk.labukdb.core.encode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Arrow {@link Schema} → Delta's JSON {@code schemaString} — the write
 * side of what every test fixture in this project has hand-written as a
 * literal string so far ({@code "{\"type\":\"struct\",\"fields\":[...]}"}).
 * {@link TableCreator} is the one real caller; built via Jackson's tree
 * API rather than string concatenation so arbitrary column names escape
 * correctly, unlike the test fixtures' shortcut.
 *
 * <b>Same supported-type set {@link ArrowToParquetWriter} already has</b> —
 * deliberately, since a column this can't express in {@code schemaString}
 * couldn't be written by {@code ArrowToParquetWriter} either: Int32,
 * Int64, Float, Double, Bool, Utf8, Timestamp, Decimal (Delta's own
 * {@code "decimal(p,s)"} type-string form), and {@code List} (Delta's
 * array type is a nested JSON object, not a type-name string — {@code
 * {"type":"array","elementType":<type>,"containsNull":<bool>}}, the same
 * shape Spark's own {@code StructType} JSON uses and Delta reuses
 * verbatim, confirmed against the Delta Sharing protocol spec and
 * delta-rs's own schema-conversion code, not assumed). Anything else
 * throws rather than silently guessing a Delta type name.
 */
public final class ArrowToDeltaSchema {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ArrowToDeltaSchema() {
    }

    public static String toSchemaString(Schema schema) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "struct");
        ArrayNode fields = root.putArray("fields");
        for (Field field : schema.getFields()) {
            ObjectNode fieldNode = MAPPER.createObjectNode();
            fieldNode.put("name", field.getName());
            fieldNode.set("type", toDeltaTypeNode(field));
            fieldNode.put("nullable", field.isNullable());
            fieldNode.set("metadata", MAPPER.createObjectNode());
            fields.add(fieldNode);
        }
        try {
            return MAPPER.writeValueAsString(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to encode schema as Delta schemaString", e);
        }
    }

    /**
     * A {@link TextNode} for every primitive type (unchanged from before),
     * or an {@link ObjectNode} {@code {"type":"array",...}} for {@code
     * List} — {@code JsonNode} covers both uniformly so the caller doesn't
     * need to know which shape a given field produces.
     */
    private static JsonNode toDeltaTypeNode(Field field) {
        if (field.getType() instanceof ArrowType.List) {
            Field elementField = field.getChildren().get(0);
            ObjectNode arrayNode = MAPPER.createObjectNode();
            arrayNode.put("type", "array");
            arrayNode.set("elementType", toDeltaTypeNode(elementField));
            arrayNode.put("containsNull", elementField.isNullable());
            return arrayNode;
        }
        return new TextNode(toDeltaTypeName(field));
    }

    private static String toDeltaTypeName(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return "integer";
            } else if (intType.getBitWidth() == 64) {
                return "long";
            }
            throw unsupported(field, type);
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "float";
                case DOUBLE -> "double";
                default -> throw unsupported(field, type);
            };
        }
        if (type instanceof ArrowType.Bool) {
            return "boolean";
        }
        if (type instanceof ArrowType.Utf8) {
            return "string";
        }
        if (type instanceof ArrowType.Timestamp) {
            return "timestamp";
        }
        if (type instanceof ArrowType.Decimal decimalType) {
            // Delta's own type-string form, e.g. "decimal(10,2)".
            return "decimal(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
        }
        throw unsupported(field, type);
    }

    private static UnsupportedOperationException unsupported(Field field, ArrowType type) {
        return new UnsupportedOperationException(
                "Arrow type not supported for Delta schemaString encoding, column '" + field.getName() + "': " + type);
    }
}
