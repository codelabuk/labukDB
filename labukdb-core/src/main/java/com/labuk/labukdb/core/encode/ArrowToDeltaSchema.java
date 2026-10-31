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
 * {@code "decimal(p,s)"} type-string form), {@code List} (Delta's array
 * type is a nested JSON object, not a type-name string — {@code
 * {"type":"array","elementType":<type>,"containsNull":<bool>}}), {@code
 * Struct} ({@code {"type":"struct","fields":[...]}}, the exact same shape
 * as the top-level schema itself — Spark/Delta's {@code StructType} JSON
 * is self-similar), and {@code Map} ({@code
 * {"type":"map","keyType":<type>,"valueType":<type>,"valueContainsNull":
 * <bool>}} — no separate {@code keyContainsNull}, since Spark's
 * {@code MapType} never allows null keys). All three nested shapes
 * confirmed against the real Delta protocol spec's "Schema Serialization
 * Format" section and {@code delta-rs}'s own schema serde test fixtures,
 * not assumed. Anything else throws rather than silently guessing a Delta
 * type name.
 */
public final class ArrowToDeltaSchema {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ArrowToDeltaSchema() {
    }

    public static String toSchemaString(Schema schema) {
        ObjectNode root = buildStructNode(schema.getFields());
        try {
            return MAPPER.writeValueAsString(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Failed to encode schema as Delta schemaString", e);
        }
    }

    /**
     * {@code {"type":"struct","fields":[...]}} — shared by the top-level
     * schema and every nested {@code Struct} field, since Delta/Spark's
     * {@code StructType} JSON is recursively self-similar.
     */
    private static ObjectNode buildStructNode(java.util.List<Field> fields) {
        ObjectNode structNode = MAPPER.createObjectNode();
        structNode.put("type", "struct");
        ArrayNode fieldsArray = structNode.putArray("fields");
        for (Field field : fields) {
            ObjectNode fieldNode = MAPPER.createObjectNode();
            fieldNode.put("name", field.getName());
            fieldNode.set("type", toDeltaTypeNode(field));
            fieldNode.put("nullable", field.isNullable());
            fieldNode.set("metadata", MAPPER.createObjectNode());
            fieldsArray.add(fieldNode);
        }
        return structNode;
    }

    /**
     * A {@link TextNode} for every primitive type, or an {@link ObjectNode}
     * for {@code List}/{@code Map}/{@code Struct} — {@code JsonNode} covers
     * both uniformly so the caller doesn't need to know which shape a given
     * field produces.
     */
    private static JsonNode toDeltaTypeNode(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.List) {
            Field elementField = field.getChildren().get(0);
            ObjectNode arrayNode = MAPPER.createObjectNode();
            arrayNode.put("type", "array");
            arrayNode.set("elementType", toDeltaTypeNode(elementField));
            arrayNode.put("containsNull", elementField.isNullable());
            return arrayNode;
        }
        if (type instanceof ArrowType.Map) {
            Field entriesField = field.getChildren().get(0);
            Field keyField = entriesField.getChildren().get(0);
            Field valueField = entriesField.getChildren().get(1);
            ObjectNode mapNode = MAPPER.createObjectNode();
            mapNode.put("type", "map");
            mapNode.set("keyType", toDeltaTypeNode(keyField));
            mapNode.set("valueType", toDeltaTypeNode(valueField));
            mapNode.put("valueContainsNull", valueField.isNullable());
            return mapNode;
        }
        if (type instanceof ArrowType.Struct) {
            return buildStructNode(field.getChildren());
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
