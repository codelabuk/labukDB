package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

/**
 * Writes {@code batch} to a new physical Parquet file under {@code
 * table}'s partition layout and builds the matching Iceberg {@link
 * DataFile} — the shared write-side half of both {@link
 * IcebergTableWriter#appendBatch} (commits via {@code newAppend()}) and
 * {@link IcebergTableMerger} (commits via {@code newOverwrite()}),
 * extracted here since both need the identical "encode + build a
 * correctly-partitioned DataFile" step and only differ in which
 * Iceberg commit API they feed it into afterward.
 */
final class IcebergDataFileWriter {

    private static final ArrowToParquetWriter PARQUET_WRITER = new ArrowToParquetWriter();

    private IcebergDataFileWriter() {
    }

    static DataFile write(Table table, TableLocation tableLocation, Map<String, String> partitionValues,
                           VectorSchemaRoot batch) throws IOException {
        String partitionPath = buildPartitionPath(table, partitionValues);
        String fileName = "part-" + UUID.randomUUID() + ".snappy.parquet";
        String relativePath = partitionPath.isEmpty() ? fileName : partitionPath + "/" + fileName;

        TableLocation dataFileLocation = tableLocation.resolve(relativePath);
        if (!dataFileLocation.isRemote()) {
            Files.createDirectories(dataFileLocation.localPath().getParent());
        }
        PARQUET_WRITER.write(dataFileLocation, batch);

        return DataFiles.builder(table.spec())
                .withPath(dataFileLocation.raw())
                .withFileSizeInBytes(fileSize(dataFileLocation))
                .withRecordCount(batch.getRowCount())
                .withPartitionPath(partitionPath)
                .build();
    }

    private static long fileSize(TableLocation location) throws IOException {
        if (!location.isRemote()) {
            return location.localPath().toFile().length();
        }
        return location.hadoopPath().getFileSystem(location.hadoopConf())
                .getFileStatus(location.hadoopPath()).getLen();
    }

    /**
     * {@code "col1=val1/col2=val2"}, ordered by the table's actual
     * partition-spec field order (not the caller's map order) — the
     * exact convention {@link IcebergTableFormat#parsePartitionPath}
     * already parses back out. Empty string for an unpartitioned table's
     * spec (no fields) — Iceberg's own convention for "no partition."
     */
    /**
     * Resolves a {@code DataFile.path()} string (always either a plain
     * local path or an {@code s3a://...} URI, per this project's own
     * write convention) back to a {@link TableLocation} for reading —
     * shared by {@link IcebergTableMerger} and {@link IcebergCompactor},
     * both of which need to re-read a table's existing data files by
     * their already-absolute Iceberg-reported path, unlike Delta's
     * relative {@code AddFile} paths resolved via {@code location.resolve}.
     */
    static TableLocation locationOf(String path, TableLocation tableLocation) {
        if (TableLocation.isRemoteUri(path)) {
            return TableLocation.forRemote(path, tableLocation.hadoopConf());
        }
        return TableLocation.forLocal(java.nio.file.Path.of(path));
    }

    private static String buildPartitionPath(Table table, Map<String, String> partitionValues) {
        StringBuilder path = new StringBuilder();
        for (PartitionField field : table.spec().fields()) {
            String value = partitionValues.get(field.name());
            if (value == null) {
                throw new IllegalArgumentException(
                        "Missing value for partition column '" + field.name() + "'; table partition columns are "
                                + table.spec().fields());
            }
            if (path.length() > 0) {
                path.append('/');
            }
            path.append(field.name()).append('=').append(value);
        }
        return path.toString();
    }
}
