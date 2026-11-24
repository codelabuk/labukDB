package com.labuk.labukdb.core.execution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Plain SQL {@code UNION} (not {@code ALL}) needs duplicate rows removed
 * across its combined branches -- no existing operator in this project
 * does this ({@link AggregateOperator}/{@link SortLimitOperator} are the
 * only comparable row-shaping operators, and neither dedupes). Equality
 * is whole-row, column-by-column, via {@code FieldVector.getObject} --
 * the same object-level equality {@code QueryCommand.printRows} already
 * trusts to represent a cell's value correctly for printing. The boxed
 * types every column kind this project supports returns (Long/Integer/
 * Double/Boolean/BigDecimal/LocalDateTime, and Arrow's own {@code Text}
 * for strings) all have real value-based {@code equals}/{@code hashCode},
 * so a plain {@code Arrays.asList} of a row's values is already a
 * correct, null-safe hash key -- no bespoke normalization needed.
 *
 * Only the first occurrence of each distinct row survives, in original
 * order -- a plain UNION's row order is otherwise implementation-defined
 * (no {@code ORDER BY} on the combined result is supported yet; see
 * {@code SqlQueryTranslator}'s own javadoc).
 */
public final class RowDeduplicator {

    public VectorSchemaRoot deduplicate(VectorSchemaRoot source, BufferAllocator allocator) {
        List<FieldVector> sourceVectors = source.getFieldVectors();
        int rowCount = source.getRowCount();

        Set<List<Object>> seen = new LinkedHashSet<>();
        List<Integer> keptRows = new ArrayList<>();
        for (int r = 0; r < rowCount; r++) {
            Object[] values = new Object[sourceVectors.size()];
            for (int c = 0; c < sourceVectors.size(); c++) {
                values[c] = sourceVectors.get(c).getObject(r);
            }
            if (seen.add(Arrays.asList(values))) {
                keptRows.add(r);
            }
        }

        VectorSchemaRoot result = VectorSchemaRoot.create(source.getSchema(), allocator);
        result.allocateNew();
        List<FieldVector> resultVectors = result.getFieldVectors();
        int outRow = 0;
        for (int sourceRow : keptRows) {
            for (int c = 0; c < sourceVectors.size(); c++) {
                resultVectors.get(c).copyFromSafe(sourceRow, outRow, sourceVectors.get(c));
            }
            outRow++;
        }
        result.setRowCount(outRow);
        return result;
    }
}
