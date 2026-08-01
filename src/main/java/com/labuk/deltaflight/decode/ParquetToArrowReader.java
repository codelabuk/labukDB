package com.labuk.deltaflight.decode;

import com.labuk.deltaflight.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Layer 3's entry point. Given one Parquet file (already selected by Layer
 * 2's file-level pruning), a column projection, a residual predicate, and
 * (optionally) that file's Hive-style partition values, this:
 *
 * <ol>
 *   <li>Opens the file and reads its footer (schema + row-group metadata).</li>
 *   <li>Prunes row groups via {@link RowGroupPruner} using footer stats.</li>
 *   <li>For each surviving row group, decodes rows and immediately checks
 *       {@link GroupPredicateEvaluator} — a row that fails is never copied
 *       into an Arrow vector.</li>
 *   <li>Fills in any requested partition columns as a constant per file
 *       (see {@link PartitionValueAppender}) — those columns don't exist in
 *       the physical file at all, so this is the only place they can come
 *       from.</li>
 * </ol>
 *
 * <b>Important precondition, worth stating rather than discovering via a
 * stack trace:</b> {@code residualPredicate} must not reference a
 * partition column by the time it reaches this class. That's not this
 * class's job to guard against — it's {@code PartitionValueSubstituter}'s,
 * called by whoever builds a per-file scan (see
 * {@code DeltaFlightProducer.getFlightInfo}) before handing a predicate
 * down this far. A predicate that still mentions a partition column here
 * would throw when {@link GroupPredicateEvaluator} tries to look it up in
 * a schema that doesn't have it.
 */
public final class ParquetToArrowReader {

    private final ParquetFooterCache footerCache = new ParquetFooterCache();

    /** Schema-only read (no partition columns, no row data) — see the 5-arg overload for the full case. */
    public Schema readSchema(File parquetFile, List<String> projectedColumns) throws IOException {
        return readSchema(parquetFile, projectedColumns, Map.of());
    }

    public Schema readSchema(File parquetFile, List<String> projectedColumns,
                              Map<String, String> partitionValues) throws IOException {
        MessageType fullSchema = footerCache.schemaOf(parquetFile);
        List<String> columns = resolveColumns(projectedColumns, fullSchema, partitionValues.keySet());
        return ArrowSchemaMapper.toArrowSchema(fullSchema, columns, partitionValues.keySet());
    }

    /** No-partition-columns convenience overload — see the 5-arg version for the Hive-partitioned case. */
    public VectorSchemaRoot read(File parquetFile, BufferAllocator allocator,
                                  List<String> projectedColumns, Predicate residualPredicate) throws IOException {
        return read(parquetFile, allocator, projectedColumns, residualPredicate, Map.of());
    }

    public VectorSchemaRoot read(File parquetFile, BufferAllocator allocator, List<String> projectedColumns,
                                  Predicate residualPredicate, Map<String, String> partitionValues) throws IOException {

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());

        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType fullSchema = fileReader.getFooter().getFileMetaData().getSchema();
            Set<String> partitionColumnNames = partitionValues.keySet();

            List<String> columns = resolveColumns(projectedColumns, fullSchema, partitionColumnNames);

            // Only physical columns go into the Parquet-level projected
            // schema — partition columns don't exist in the file, so they
            // must never reach ColumnIOFactory/RecordReader.
            List<String> physicalColumns = columns.stream()
                    .filter(c -> ArrowSchemaMapper.containsField(fullSchema, c))
                    .collect(Collectors.toList());
            List<Type> selectedTypes = fullSchema.getFields().stream()
                    .filter(t -> physicalColumns.contains(t.getName()))
                    .collect(Collectors.toList());
            MessageType projectedSchema = new MessageType(fullSchema.getName(), selectedTypes);

            Schema arrowSchema = ArrowSchemaMapper.toArrowSchema(fullSchema, columns, partitionColumnNames);
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            root.allocateNew();

            List<CellAppender> appenders = new ArrayList<>();
            for (String column : columns) {
                FieldVector vector = root.getVector(column);
                if (physicalColumns.contains(column)) {
                    PrimitiveType.PrimitiveTypeName typeName =
                            projectedSchema.getType(column).asPrimitiveType().getPrimitiveTypeName();
                    appenders.add(new RowAppender(column, typeName, vector));
                } else {
                    appenders.add(new PartitionValueAppender((VarCharVector) vector, partitionValues.get(column)));
                }
            }

            List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
            boolean[] keepRowGroup = RowGroupPruner.pruneMask(blocks, residualPredicate);

            int destRow = 0;
            for (int i = 0; i < blocks.size(); i++) {
                if (!keepRowGroup[i]) {
                    fileReader.skipNextRowGroup();
                    continue;
                }

                PageReadStore pages = fileReader.readNextRowGroup();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectedSchema, fullSchema);
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(projectedSchema));

                long rowCountInGroup = pages.getRowCount();
                for (long r = 0; r < rowCountInGroup; r++) {
                    Group row = recordReader.read();
                    if (GroupPredicateEvaluator.matches(residualPredicate, row, projectedSchema)) {
                        for (CellAppender appender : appenders) {
                            appender.append(row, destRow);
                        }
                        destRow++;
                    }
                }
            }

            root.setRowCount(destRow);
            return root;
        }
    }

    /**
     * Default projection is "every physical column, plus every partition
     * column" — a real "select *" against a Hive-partitioned table
     * includes the partition columns, not just the file's own fields.
     */
    private static List<String> resolveColumns(List<String> projectedColumns, MessageType fullSchema,
                                                 Set<String> partitionColumnNames) {
        if (projectedColumns != null && !projectedColumns.isEmpty()) {
            return projectedColumns;
        }
        Set<String> defaultColumns = new LinkedHashSet<>();
        fullSchema.getFields().forEach(f -> defaultColumns.add(f.getName()));
        defaultColumns.addAll(partitionColumnNames);
        return new ArrayList<>(defaultColumns);
    }
}
