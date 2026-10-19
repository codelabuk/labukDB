package com.labuk.labukdb.core.decode;

import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.example.data.Group;

import java.nio.charset.StandardCharsets;

/**
 * The Hive-style partition-column half of {@link CellAppender}: the value
 * is fixed for the whole file (that's what a partition value means — every
 * row under {@code level=ERROR/} has {@code level == "ERROR"}), so this
 * writes the same bytes at every destination index and never looks at the
 * decoded row at all.
 *
 * Always UTF-8/string-typed — see {@code ArrowSchemaMapper}'s class
 * javadoc for why partition values aren't reparsed back into their
 * original logical type here.
 */
final class PartitionValueAppender implements CellAppender {

    private final VarCharVector vector;
    private final byte[] valueBytes;

    PartitionValueAppender(VarCharVector vector, String partitionValue) {
        this.vector = vector;
        // partitionValue is null only if a caller asks for a partition
        // column that genuinely has no value recorded for this file, which
        // shouldn't happen given how ParquetToArrowReader builds these -
        // but a null-safe encode here is one line, versus a confusing NPE
        // deep in a hot loop if that assumption is ever wrong.
        this.valueBytes = (partitionValue == null) ? null : partitionValue.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void append(Group row, int destIndex) {
        if (valueBytes == null) {
            vector.setNull(destIndex);
        } else {
            vector.setSafe(destIndex, valueBytes);
        }
    }
}
