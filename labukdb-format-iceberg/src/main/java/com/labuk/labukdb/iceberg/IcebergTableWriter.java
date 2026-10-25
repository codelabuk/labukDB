package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

/**
 * Appends a batch to an <b>already-existing</b> Iceberg table — the
 * Iceberg counterpart of {@code log.CommitWriter}, same
 * constructor-and-method shape for discoverability, but structurally
 * simpler: Iceberg's own {@code AppendFiles#commit()} already runs a
 * bounded, exponential-backoff optimistic-concurrency retry internally
 * (confirmed against {@code SnapshotProducer.commit()}'s real bytecode,
 * not assumed) — manifest writing, snapshot creation, and the atomic
 * catalog-pointer swap are all handled by {@code iceberg-core} itself.
 * Unlike {@code CommitWriter}, nothing here needs its own retry loop.
 *
 * <b>Deliberately scoped to append only, matching how Delta's own write
 * path was built incrementally</b> (single-writer commit path first,
 * {@code TableCreator} much later, after append/merge/compact/vacuum
 * already worked) — no CREATE TABLE, no merge/upsert, no compaction, no
 * vacuum. The table must already exist (via {@code HadoopTables.create}
 * or equivalent external Iceberg tooling).
 */
public final class IcebergTableWriter {

    private final Table table;
    private final TableLocation tableLocation;
    private final ArrowToParquetWriter parquetWriter = new ArrowToParquetWriter();

    /**
     * Local or remote (S3/MinIO/ADLS), same dispatch {@link
     * IcebergTableFormat}'s own {@code TableLocation} constructor uses —
     * an empty default {@code Configuration} is sufficient for local
     * Hadoop {@code FileSystem} access when the location isn't remote.
     */
    public IcebergTableWriter(TableLocation location) {
        this.tableLocation = location;
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        this.table = new HadoopTables(conf).load(location.raw());
    }

    /**
     * Writes {@code batch} to a new Parquet file (via the existing,
     * unmodified {@code ArrowToParquetWriter}) and commits it as one new
     * Iceberg snapshot. The table's current {@link Table#spec()} decides
     * both the physical partition-directory layout and the {@code
     * DataFile}'s declared partition — a file written under one spec
     * stays correct even if the table's spec evolves later, the same way
     * {@link IcebergTableFormat#toDataFileRef} already reads a file back
     * against the spec it was written under, not the table's latest one.
     */
    public void appendBatch(VectorSchemaRoot batch, Map<String, String> partitionValues) throws IOException {
        String partitionPath = buildPartitionPath(partitionValues);
        String fileName = "part-" + UUID.randomUUID() + ".snappy.parquet";
        String relativePath = partitionPath.isEmpty() ? fileName : partitionPath + "/" + fileName;

        TableLocation dataFileLocation = tableLocation.resolve(relativePath);
        if (!dataFileLocation.isRemote()) {
            Files.createDirectories(dataFileLocation.localPath().getParent());
        }
        parquetWriter.write(dataFileLocation, batch);

        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(dataFileLocation.raw())
                .withFileSizeInBytes(fileSize(dataFileLocation))
                .withRecordCount(batch.getRowCount())
                .withPartitionPath(partitionPath)
                .build();

        table.newAppend().appendFile(dataFile).commit();
    }

    private long fileSize(TableLocation location) throws IOException {
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
     * already parses back out, and the same Hive-style layout {@code
     * DataFiles.Builder#withPartitionPath} already accepts (proven
     * against both local and remote fixtures in {@code
     * IcebergTableFormatTest}/{@code IcebergRemoteMinioIntegrationTest}).
     * Empty string for an unpartitioned table's spec (no fields) —
     * Iceberg's own convention for "no partition."
     */
    private String buildPartitionPath(Map<String, String> partitionValues) {
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
