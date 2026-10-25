package com.labuk.labukdb.core.execution;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.IOException;
import java.util.List;

/**
 * The "answer a query that can't be served file-by-file independently"
 * executor — any query with {@code GROUP BY}/aggregates/{@code ORDER BY}/
 * {@code LIMIT} needs to see rows across every surviving file before
 * producing a result, unlike a plain scan. Shared by {@code labukdb-flight}
 * (one combined {@code DoGet} endpoint) and {@code labukdb-cli} (SQL
 * queries via {@code QueryCommand}) so both call one real implementation
 * instead of two copies that could drift — extracted from
 * {@code LabukDbFlightProducer}'s original inline combine-mode logic
 * verbatim, not rewritten.
 *
 * Decodes every file and combines them into one root (same
 * {@link VectorSchemaRootAppender} pattern {@code Compactor}/{@code TableMerger}
 * already use, including the {@code allocateNew()} gotcha — a bare
 * {@code create()} alone leaves zero-capacity buffers and fails on the
 * first variable-width column appended into it), then optionally
 * {@link AggregateOperator#aggregate}, then optionally
 * {@link SortLimitOperator#sortAndLimit}. Callers own the returned root
 * and must close it.
 */
public final class CombinedQueryExecutor {

    private final ParquetToArrowReader parquetReader = new ParquetToArrowReader();

    public VectorSchemaRoot execute(List<FileScan> files, List<String> projectedColumns,
                                     List<String> groupByColumns, List<AggregateSpec> aggregates,
                                     List<SortKey> sortKeys, long limit,
                                     BufferAllocator allocator) throws IOException {
        VectorSchemaRoot current = null;
        try {
            for (FileScan file : files) {
                try (VectorSchemaRoot batch = parquetReader.read(
                        file.dataFile(), allocator, projectedColumns,
                        file.residualPredicate(), file.partitionValues())) {
                    if (current == null) {
                        current = VectorSchemaRoot.create(batch.getSchema(), allocator);
                        current.allocateNew();
                    }
                    VectorSchemaRootAppender.append(current, batch);
                }
            }

            if (!groupByColumns.isEmpty() || !aggregates.isEmpty()) {
                VectorSchemaRoot aggregated = new AggregateOperator().aggregate(
                        List.of(current), groupByColumns, aggregates, allocator);
                current.close();
                current = aggregated;
            }
            if (!sortKeys.isEmpty() || limit >= 0) {
                VectorSchemaRoot sorted = new SortLimitOperator().sortAndLimit(
                        current, sortKeys, limit, allocator);
                current.close();
                current = sorted;
            }

            VectorSchemaRoot result = current;
            current = null; // ownership transferred to the caller
            return result;
        } finally {
            if (current != null) {
                current.close();
            }
        }
    }
}
