package com.labuk.deltaflight.decode;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translates a Parquet {@code MessageType} into an Arrow {@code Schema} for
 * a given column projection.
 *
 * <b>Scope, stated plainly:</b> flat primitive columns only — INT32, INT64,
 * FLOAT, DOUBLE, BOOLEAN, and BINARY (treated as UTF-8 string; this engine
 * doesn't yet distinguish Parquet's raw BINARY from its UTF8-annotated
 * BINARY, since every column this project's fixtures produce is text).
 * Not yet supported: DECIMAL, INT96 (legacy timestamp encoding), and any
 * nested/repeated (LIST, MAP, GROUP) structure.
 *
 * <b>This matters concretely for this project's own data:</b> Spark writes
 * TIMESTAMP columns as INT96 by default. Unless the writer sets
 * {@code spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS} (which
 * encodes as INT64 and IS supported here), a timestamp column like
 * {@code event_time} from local-lakehouse-lab's own Spark job will hit the
 * UnsupportedOperationException below. That's a real, not hypothetical,
 * integration point to check before running this against real data.
 */
final class ArrowSchemaMapper {

    private ArrowSchemaMapper() {
    }

    static Schema toArrowSchema(MessageType parquetSchema, List<String> projectedColumns) {
        List<String> columns = (projectedColumns == null || projectedColumns.isEmpty())
                ? parquetSchema.getFields().stream().map(Type::getName).collect(Collectors.toList())
                : projectedColumns;

        List<Field> fields = new ArrayList<>();
        for (String column : columns) {
            PrimitiveTypeName typeName = parquetSchema.getType(column).asPrimitiveType().getPrimitiveTypeName();
            fields.add(new Field(column, FieldType.nullable(toArrowType(typeName, column)), null));
        }
        return new Schema(fields);
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
            default:
                throw new UnsupportedOperationException(
                        "Parquet type not supported yet: " + typeName + " (column '" + column + "'). "
                                + "See ArrowSchemaMapper's class javadoc for the current scope.");
        }
    }
}
