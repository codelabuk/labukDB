package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL engine Phase 2's real proof: {@link QueryCommand#scanAndPrintUpToLimit}
 * must stop opening files once {@code limit} rows are satisfied — not
 * merely "print the right row count," which a buggy implementation that
 * reads every file first and truncates at the end would also get right.
 * File 2 here is a {@link TableLocation} that doesn't exist on disk at
 * all; the only way this test passes is if {@code scanAndPrintUpToLimit}
 * genuinely never tries to open it, since doing so would throw.
 */
class QueryCommandScanAndPrintUpToLimitTest {

    private static final Field AMOUNT = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);

    private File writeFiveRowFile(Path dir) throws Exception {
        File file = dir.resolve("part-0.parquet").toFile();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(AMOUNT)), allocator)) {
            batch.allocateNew();
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < 5; i++) {
                amountVector.setSafe(i, i + 1L);
            }
            batch.setRowCount(5);
            new ArrowToParquetWriter().write(file, batch);
        }
        return file;
    }

    @Test
    void stopsBeforeOpeningALaterFile_onceLimitSatisfied(@TempDir Path tempDir) throws Exception {
        File realFile = writeFiveRowFile(tempDir);
        TableLocation realLocation = TableLocation.forLocal(realFile.toPath());
        TableLocation nonExistentLocation = TableLocation.forLocal(tempDir.resolve("does-not-exist.parquet"));

        List<FileScan> files = List.of(
                new FileScan(realLocation, null, Map.of()),
                new FileScan(nonExistentLocation, null, Map.of()));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // limit=3 < the real file's 5 rows -- satisfied entirely by file 1;
            // if this ever tried to open file 2 it would throw (the file doesn't exist).
            assertDoesNotThrow(() ->
                    QueryCommand.scanAndPrintUpToLimit(files, List.of("amount"), 3, allocator));
        }
    }

    @Test
    void limitZero_printsZeroRowsWithoutOpeningAnyFile(@TempDir Path tempDir) throws Exception {
        TableLocation nonExistentLocation = TableLocation.forLocal(tempDir.resolve("does-not-exist.parquet"));
        List<FileScan> files = List.of(new FileScan(nonExistentLocation, null, Map.of()));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            assertDoesNotThrow(() ->
                    QueryCommand.scanAndPrintUpToLimit(files, List.of("amount"), 0, allocator));
        }
    }
}
