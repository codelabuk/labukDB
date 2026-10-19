package com.labuk.labukdb.core.decode;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.nio.charset.StandardCharsets;

/**
 * Appends one column's value, for one row, from a decoded Parquet
 * {@code Group} into the matching Arrow vector at a given destination
 * index. Appends rather than random-writes, since destination indices
 * advance only for rows that pass {@link GroupPredicateEvaluator} - the
 * residual filter and the decode step are fused into one pass rather than
 * decoding everything and filtering after, which avoids ever materializing
 * Arrow values for rows that are going to be thrown away anyway.
 *
 * One of two {@link CellAppender} implementations - see that interface's
 * javadoc for the other (partition-column) case.
 */
final class RowAppender implements CellAppender {

    private final String column;
    private final PrimitiveTypeName typeName;
    private final FieldVector vector;

    RowAppender(String column, PrimitiveTypeName typeName, FieldVector vector) {
        this.column = column;
        this.typeName = typeName;
        this.vector = vector;
    }

    @Override
    public void append(Group row, int destIndex) {
        boolean isNull = row.getFieldRepetitionCount(column) == 0;
        switch (typeName) {
            case INT32: {
                IntVector v = (IntVector) vector;
                if (isNull) v.setNull(destIndex);
                else v.setSafe(destIndex, row.getInteger(column, 0));
                break;
            }
            case INT64: {
                BigIntVector v = (BigIntVector) vector;
                if (isNull) v.setNull(destIndex);
                else v.setSafe(destIndex, row.getLong(column, 0));
                break;
            }
            case FLOAT: {
                Float4Vector v = (Float4Vector) vector;
                if (isNull) v.setNull(destIndex);
                else v.setSafe(destIndex, row.getFloat(column, 0));
                break;
            }
            case DOUBLE: {
                Float8Vector v = (Float8Vector) vector;
                if (isNull) v.setNull(destIndex);
                else v.setSafe(destIndex, row.getDouble(column, 0));
                break;
            }
            case BOOLEAN: {
                BitVector v = (BitVector) vector;
                if (isNull) v.setNull(destIndex);
                else v.setSafe(destIndex, row.getBoolean(column, 0) ? 1 : 0);
                break;
            }
            case BINARY: {
                VarCharVector v = (VarCharVector) vector;
                if (isNull) {
                    v.setNull(destIndex);
                } else {
                    byte[] bytes = row.getString(column, 0).getBytes(StandardCharsets.UTF_8);
                    v.setSafe(destIndex, bytes);
                }
                break;
            }
            case INT96: {
                TimeStampMicroVector v = (TimeStampMicroVector) vector;
                if (isNull) {
                    v.setNull(destIndex);
                } else {
                    Binary raw = row.getInt96(column, 0);
                    v.setSafe(destIndex, Int96TimestampCodec.toEpochMicros(raw));
                }
                break;
            }
            default:
                throw new UnsupportedOperationException("Parquet type not supported yet: " + typeName);
        }
    }
}
