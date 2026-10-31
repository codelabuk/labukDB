package com.labuk.labukdb.core.decode;

import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.PredicateColumns;
import com.labuk.labukdb.core.storage.TableLocation;
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
import org.apache.parquet.schema.LogicalTypeAnnotation;
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

    // --- TableLocation-based: dispatches local vs. remote, the entry point LabukDbFlightProducer uses ---

    /**
     * Dispatches on the location - a local path uses the cached
     * {@code File} path (keeping {@link ParquetFooterCache}'s benefit), a
     * remote URI builds a Hadoop-backed {@code InputFile}. This is what
     * makes an end-to-end remote (S3/MinIO/ADLS) query actually work,
     * rather than only the log layer being remote-capable.
     */
    public Schema readSchema(TableLocation dataFile, List<String> projectedColumns,
                              Map<String, String> partitionValues) throws IOException {
        if (dataFile.isRemote()) {
            return readSchema(ParquetInputFiles.remote(dataFile.hadoopPath(), dataFile.hadoopConf()),
                    projectedColumns, partitionValues);
        }
        return readSchema(dataFile.localPath().toFile(), projectedColumns, partitionValues);
    }

    public VectorSchemaRoot read(TableLocation dataFile, BufferAllocator allocator, List<String> projectedColumns,
                                  Predicate residualPredicate, Map<String, String> partitionValues) throws IOException {
        if (dataFile.isRemote()) {
            return read(ParquetInputFiles.remote(dataFile.hadoopPath(), dataFile.hadoopConf()),
                    allocator, projectedColumns, residualPredicate, partitionValues);
        }
        return read(dataFile.localPath().toFile(), allocator, projectedColumns, residualPredicate, partitionValues);
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

            // Output columns — what the caller asked to receive back.
            List<String> outputColumns = resolveColumns(projectedColumns, fullSchema, partitionColumnNames);

            // Physical read set = output columns UNION predicate columns.
            // This is the fix for the projection-pushdown bug: a predicate
            // on is_active with output columns [company, title] previously
            // built a read schema with only company/title, then tried to
            // evaluate is_active in GroupPredicateEvaluator against a row
            // where is_active was never decoded — InvalidRecordException.
            // We read the extra predicate column(s), filter, then drop them
            // before returning so the output schema still only contains what
            // the caller asked for.
            Set<String> predicateColumnNames = PredicateColumns.collect(residualPredicate);
            List<String> physicalOutputColumns = new ArrayList<>(outputColumns);
            List<String> predicateOnlyColumns = new ArrayList<>();
            for (String col : predicateColumnNames) {
                if (!outputColumns.contains(col)
                        && !partitionColumnNames.contains(col)
                        && ArrowSchemaMapper.containsField(fullSchema, col)) {
                    physicalOutputColumns.add(col);
                    predicateOnlyColumns.add(col);
                }
            }

            List<String> physicalColumns = physicalOutputColumns.stream()
                    .filter(c -> ArrowSchemaMapper.containsField(fullSchema, c))
                    .collect(Collectors.toList());
            List<Type> selectedTypes = fullSchema.getFields().stream()
                    .filter(t -> physicalColumns.contains(t.getName()))
                    .collect(Collectors.toList());
            MessageType projectedSchema = new MessageType(fullSchema.getName(), selectedTypes);

            // Arrow schema for the internal decode — includes predicate-only
            // columns so GroupPredicateEvaluator can evaluate them.
            Schema decodeArrowSchema = ArrowSchemaMapper.toArrowSchema(fullSchema, physicalOutputColumns, partitionColumnNames);
            VectorSchemaRoot decodeRoot = VectorSchemaRoot.create(decodeArrowSchema, allocator);
            decodeRoot.allocateNew();

            List<CellAppender> appenders = new ArrayList<>();
            for (String column : physicalOutputColumns) {
                FieldVector vector = decodeRoot.getVector(column);
                if (physicalColumns.contains(column)) {
                    Type parquetType = projectedSchema.getType(column);
                    if (!parquetType.isPrimitive() && NestedTypeDecoder.isSupported(parquetType)) {
                        // Nested type (LIST/MAP/STRUCT, any depth) -- the cast to the
                        // right vector subtype lives inside NestedTypeDecoder itself.
                        final Type capturedType = parquetType;
                        final FieldVector capturedVector = vector;
                        final String capturedColumn = column;
                        appenders.add((row, destIndex) ->
                                NestedTypeDecoder.appendValue(row, capturedColumn, capturedType, capturedVector, destIndex));
                    } else {
                        PrimitiveType primitiveType = parquetType.asPrimitiveType();
                        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
                        Integer decimalScale = null;
                        if (primitiveType.getLogicalTypeAnnotation()
                                instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
                            decimalScale = decimalType.getScale();
                        }
                        appenders.add(new RowAppender(column, typeName, vector, decimalScale));
                    }
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

            decodeRoot.setRowCount(destRow);

            // Drop predicate-only columns from the output so the caller
            // only sees what they asked for, even though we read more.
            if (predicateOnlyColumns.isEmpty()) {
                return decodeRoot;
            }
            Schema outputArrowSchema = ArrowSchemaMapper.toArrowSchema(fullSchema, outputColumns, partitionColumnNames);
            VectorSchemaRoot outputRoot = VectorSchemaRoot.create(outputArrowSchema, allocator);
            outputRoot.allocateNew();
            for (String col : outputColumns) {
                FieldVector src = decodeRoot.getVector(col);
                FieldVector dst = outputRoot.getVector(col);
                for (int r = 0; r < destRow; r++) {
                    dst.copyFromSafe(r, r, src);
                }
            }
            outputRoot.setRowCount(destRow);
            decodeRoot.close();
            return outputRoot;
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
