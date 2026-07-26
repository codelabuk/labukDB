package com.labuk.deltaflight.decode;

import com.labuk.deltaflight.planner.Predicate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Layer 3's entry point. Given one Parquet file (already selected by Layer
 * 2's file-level pruning) plus a projection and a residual predicate, this:
 *
 * <ol>
 *   <li>Opens the file and reads its footer (schema + row-group metadata) —
 *       no row data touched yet.</li>
 *   <li>Prunes row groups via {@link RowGroupPruner} using footer
 *       statistics — Parquet's own {@code skipNextRowGroup()} means a
 *       pruned row group's pages are never even read off disk.</li>
 *   <li>For each surviving row group, decodes rows one at a time and
 *       immediately checks {@link GroupPredicateEvaluator} — a row that
 *       fails is simply never copied into an Arrow vector, rather than
 *       being decoded fully and then discarded.</li>
 * </ol>
 *
 * Column projection happens by only including requested columns in the
 * schema handed to Parquet's column reader machinery in the first place —
 * Parquet's on-disk column-chunk layout means unrequested columns' bytes
 * are never read, not just discarded after decode.
 *
 * See {@link ArrowSchemaMapper} for the current column-type scope
 * (flat primitives only; no DECIMAL, INT96, or nested types yet).
 */
public final class ParquetToArrowReader {

    /**
     * Reads only the file's footer to produce its Arrow schema — no row
     * group is opened or decoded. Used by callers (the Flight producer's
     * {@code GetFlightInfo}) that need to describe a query's result shape
     * without paying for a full scan.
     */
    public Schema readSchema(File parquetFile, List<String> projectedColumns) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType fullSchema = fileReader.getFooter().getFileMetaData().getSchema();
            List<String> columns = (projectedColumns == null || projectedColumns.isEmpty())
                    ? fullSchema.getFields().stream().map(Type::getName).collect(Collectors.toList())
                    : projectedColumns;
            return ArrowSchemaMapper.toArrowSchema(fullSchema, columns);
        }
    }

    public VectorSchemaRoot read(File parquetFile, BufferAllocator allocator,
                                  List<String> projectedColumns, Predicate residualPredicate) throws IOException {

        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());

        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType fullSchema = fileReader.getFooter().getFileMetaData().getSchema();

            List<String> columns = (projectedColumns == null || projectedColumns.isEmpty())
                    ? fullSchema.getFields().stream().map(Type::getName).collect(Collectors.toList())
                    : projectedColumns;

            List<Type> selectedTypes = fullSchema.getFields().stream()
                    .filter(t -> columns.contains(t.getName()))
                    .collect(Collectors.toList());
            MessageType projectedSchema = new MessageType(fullSchema.getName(), selectedTypes);

            Schema arrowSchema = ArrowSchemaMapper.toArrowSchema(fullSchema, columns);
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            root.allocateNew();

            List<RowAppender> appenders = new ArrayList<>();
            for (String column : columns) {
                PrimitiveType.PrimitiveTypeName typeName =
                        projectedSchema.getType(column).asPrimitiveType().getPrimitiveTypeName();
                FieldVector vector = root.getVector(column);
                appenders.add(new RowAppender(column, typeName, vector));
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
                        for (RowAppender appender : appenders) {
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
}
