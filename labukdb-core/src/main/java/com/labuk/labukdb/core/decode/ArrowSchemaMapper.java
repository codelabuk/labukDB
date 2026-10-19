package com.labuk.labukdb.core.decode;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Translates a requested column list into an Arrow {@code Schema}, where
 * each column is either a physical Parquet field (typed from the file's
 * own footer) or a Hive-style partition column (typed as UTF-8 - Delta's
 * log stores partition values as strings regardless of the column's
 * logical type, and this engine doesn't attempt to reparse them back into
 * their original type; a caller wanting {@code log_date} as an actual date
 * type rather than the literal string {@code "2026-07-17"} would need to
 * cast it downstream).
 *
 * <b>Scope, stated plainly, for physical columns:</b> flat primitives only
 * - INT32, INT64, FLOAT, DOUBLE, BOOLEAN, INT96 (legacy timestamp - see
 * {@link Int96TimestampCodec}), and BINARY (treated as UTF-8 string).
 * Not yet supported: DECIMAL, and any nested/repeated (LIST, MAP, GROUP)
 * structure.
 */
final class ArrowSchemaMapper {

    private ArrowSchemaMapper() {
    }

    /**
     * @param columns              every requested output column, physical or partition
     * @param partitionColumnNames which of the table's columns are partition columns
     *                              (not necessarily all requested - just the table's set)
     */
    static Schema toArrowSchema(MessageType parquetSchema, List<String> columns, Set<String> partitionColumnNames) {
        List<Field> fields = new ArrayList<>();
        for (String column : columns) {
            if (containsField(parquetSchema, column)) {
                Type parquetType = parquetSchema.getType(column);
                if (parquetType.isPrimitive()) {
                    PrimitiveTypeName typeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
                    fields.add(new Field(column, FieldType.nullable(toArrowType(typeName, column)), null));
                } else if (NestedTypeDecoder.isSupported(parquetType)) {
                    // Nested type (LIST<float> etc.) — delegate to NestedTypeDecoder
                    // for the Arrow field definition so the schema matches
                    // what the decode step will produce.
                    fields.add(NestedTypeDecoder.toArrowField(column, parquetType));
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported Parquet type for column '" + column + "': " + parquetType
                                    + ". Supported nested types: LIST<float/double/int/long/string>.");
                }
            } else if (partitionColumnNames.contains(column)) {
                fields.add(new Field(column, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            } else {
                throw new IllegalArgumentException(
                        "Column '" + column + "' is neither a physical field in this file nor a known partition column");
            }
        }
        return new Schema(fields);
    }

    static boolean containsField(MessageType parquetSchema, String column) {
        for (Type field : parquetSchema.getFields()) {
            if (field.getName().equals(column)) {
                return true;
            }
        }
        return false;
    }

    static ArrowType toArrowType(PrimitiveTypeName typeName, String column) {
        switch (typeName) {
            case INT32:
                return new ArrowType.Int(32, true);
            case INT64:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case BINARY:
                return ArrowType.Utf8.INSTANCE;
            case INT96:
                // See Int96TimestampCodec for the decode itself and the
                // timezone caveat (this reads INT96 as UTC directly, no
                // rebasing - check spark.sql.parquet.int96TimestampConversion
                // on the writer side if values come back shifted).
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            default:
                throw new UnsupportedOperationException(
                        "Parquet type not supported yet: " + typeName + " (column '" + column + "'). "
                                + "See ArrowSchemaMapper's class javadoc for the current scope.");
        }
    }
}
