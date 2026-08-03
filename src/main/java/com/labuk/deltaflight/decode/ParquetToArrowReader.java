package com.labuk.deltaflight.decode;

import com.labuk.deltaflight.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
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
 *       {@link GroupPredicateEvaluator} - a row that fails is never copied
 *       into an Arrow vector.</li>
 *   <li>Fills in any requested partition columns as a constant per file
 *       (see {@link PartitionValueAppender}).</li>
 * </ol>
 *
 * <b>Storage-agnostic as of this version</b> - every method takes an
 * {@link InputFile} internally (parquet-mr's own storage abstraction,
 * already implemented for local and Hadoop/S3/MinIO/ADLS via
 * {@link ParquetInputFiles}). The original {@code File}-based methods are
 * kept, unchanged in behavior, as thin wrappers - nothing that already
 * calls this class needs to change.
 *
 * <b>Important precondition:</b> {@code residualPredicate} must not
 * reference a partition column by the time it reaches this class - see
 * {@code PartitionValueSubstituter}, called by whoever builds a per-file
 * scan, for why.
 */
public final class ParquetToArrowReader {

    private final ParquetFooterCache footerCache = new ParquetFooterCache();

    // --- File-based methods: unchanged behavior, now thin wrappers ---

    public Schema readSchema(File parquetFile, List<String> projectedColumns) throws IOException {
        return readSchema(parquetFile, projectedColumns, Map.of());
    }

    public Schema readSchema(File parquetFile, List<String> projectedColumns,
                              Map<String, String> partitionValues) throws IOException {
        // Deliberately NOT delegating to the InputFile overload below — that
        // one doesn't use ParquetFooterCache (no cache key concept for a
        // generic remote InputFile yet), and this is the one path where the
        // cache is actually proven and tested (ParquetFooterCacheTest).
        // Losing that here would be a real, silent performance regression
        // for the local case GetFlightInfo calls on every single query.
        MessageType fullSchema = footerCache.schemaOf(parquetFile);
        List<String> columns = resolveColumns(projectedColumns, fullSchema, partitionValues.keySet());
        return ArrowSchemaMapper.toArrowSchema(fullSchema, columns, partitionValues.keySet());
    }

    public VectorSchemaRoot read(File parquetFile, BufferAllocator allocator,
                                  List<String> projectedColumns, Predicate residualPredicate) throws IOException {
        return read(parquetFile, allocator, projectedColumns, residualPredicate, Map.of());
    }

    public VectorSchemaRoot read(File parquetFile, BufferAllocator allocator, List<String> projectedColumns,
                                  Predicate residualPredicate, Map<String, String> partitionValues) throws IOException {
        return read(ParquetInputFiles.local(parquetFile), allocator, projectedColumns, residualPredicate, partitionValues);
    }

    // --- InputFile-based methods: the actual implementation, storage-agnostic ---

    /** Uses {@link ParquetFooterCache}, so only meaningful for a stable local path — not used for the InputFile case, which always re-reads the footer (no cache key concept for a remote InputFile yet). */
    public Schema readSchema(InputFile inputFile, List<String> projectedColumns,
                              Map<String, String> partitionValues) throws IOException {
        MessageType fullSchema = readFooterSchema(inputFile);
        List<String> columns = resolveColumns(projectedColumns, fullSchema, partitionValues.keySet());
        return ArrowSchemaMapper.toArrowSchema(fullSchema, columns, partitionValues.keySet());
    }

    public VectorSchemaRoot read(InputFile inputFile, BufferAllocator allocator, List<String> projectedColumns,
                                  Predicate residualPredicate, Map<String, String> partitionValues) throws IOException {

        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType fullSchema = fileReader.getFooter().getFileMetaData().getSchema();
            Set<String> partitionColumnNames = partitionValues.keySet();

            List<String> columns = resolveColumns(projectedColumns, fullSchema, partitionColumnNames);

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

    private MessageType readFooterSchema(InputFile inputFile) throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            return fileReader.getFooter().getFileMetaData().getSchema();
        }
    }

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
