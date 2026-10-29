package com.labuk.labukdb.iceberg;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Arrow {@link Schema} → Iceberg {@link org.apache.iceberg.Schema} — the
 * Iceberg counterpart of {@code core.encode.ArrowToDeltaSchema}, same
 * supported-type set deliberately: Int32, Int64, Float, Double, Bool,
 * Utf8, Timestamp, Decimal (precision ≤38 — Iceberg's own {@code
 * DecimalType} enforces this itself), and {@code List<primitive>} (the
 * exact set {@code core.encode.ArrowToParquetWriter} can actually
 * physically write — a column this can't express couldn't be written
 * either, the same reasoning {@code ArrowToDeltaSchema}'s own javadoc
 * states). Anything else throws rather than silently guessing an Iceberg
 * type.
 *
 * <b>Field IDs: one running counter threaded through the whole recursive
 * build, assigned before use</b> — Iceberg requires globally unique field
 * IDs across the whole schema, including nested types (a {@code List}'s
 * element needs its own ID separate from the list field's own).
 * Originally tried building every field with a placeholder ID and fixing
 * them up afterward via {@code TypeUtil.assignFreshIds} — found the hard
 * way (a real {@code IllegalArgumentException: Multiple entries with
 * same key}, not assumed) that {@code Schema}'s own constructor eagerly
 * builds and validates an id→name index immediately, before any
 * after-the-fact reassignment ever gets a chance to run — so IDs must
 * already be unique at construction time, not patched up after.
 */
final class ArrowToIcebergSchema {

    private ArrowToIcebergSchema() {
    }

    static org.apache.iceberg.Schema toIcebergSchema(Schema arrowSchema) {
        AtomicInteger nextId = new AtomicInteger(0);
        List<Types.NestedField> fields = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            fields.add(toNestedField(field, nextId));
        }
        return new org.apache.iceberg.Schema(fields);
    }

    private static Types.NestedField toNestedField(Field field, AtomicInteger nextId) {
        int id = nextId.incrementAndGet();
        Type icebergType = toIcebergType(field, nextId);
        return field.isNullable()
                ? Types.NestedField.optional(id, field.getName(), icebergType)
                : Types.NestedField.required(id, field.getName(), icebergType);
    }

    private static Type toIcebergType(Field field, AtomicInteger nextId) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return Types.IntegerType.get();
            } else if (intType.getBitWidth() == 64) {
                return Types.LongType.get();
            }
            throw unsupported(field, type);
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> Types.FloatType.get();
                case DOUBLE -> Types.DoubleType.get();
                default -> throw unsupported(field, type);
            };
        }
        if (type instanceof ArrowType.Bool) {
            return Types.BooleanType.get();
        }
        if (type instanceof ArrowType.Utf8) {
            return Types.StringType.get();
        }
        if (type instanceof ArrowType.Timestamp) {
            // core.encode.ArrowToParquetWriter always writes
            // isAdjustedToUTC=true (see its own javadoc) regardless of the
            // Arrow field's own timezone string -- withZone() matches what
            // this engine's own writer actually produces, not a general
            // Arrow-timezone-string mapping.
            return Types.TimestampType.withZone();
        }
        if (type instanceof ArrowType.Decimal decimalType) {
            return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof ArrowType.List) {
            Field elementField = field.getChildren().get(0);
            int elementId = nextId.incrementAndGet();
            Type elementType = toIcebergType(elementField, nextId);
            return elementField.isNullable()
                    ? Types.ListType.ofOptional(elementId, elementType)
                    : Types.ListType.ofRequired(elementId, elementType);
        }
        throw unsupported(field, type);
    }

    private static UnsupportedOperationException unsupported(Field field, ArrowType type) {
        return new UnsupportedOperationException(
                "Arrow type not supported for Iceberg schema encoding, column '" + field.getName() + "': " + type);
    }
}
