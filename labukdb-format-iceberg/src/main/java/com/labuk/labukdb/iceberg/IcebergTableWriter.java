package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
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
 *
 * <h2>Path-based ({@link #IcebergTableWriter(TableLocation)}): confirmed
 * broken under genuine concurrent writers on real MinIO — root-caused,
 * not just documented as a risk.</h2>
 * A real 8-separate-process stress test against real MinIO found 6 of 8
 * concurrent appends silently lost — each writer reported success, but
 * only 2 of the 8 files ended up visible in the final table (see {@code
 * IcebergTableWriterConcurrentMinioIntegrationTest}). Root cause, traced
 * through {@code iceberg-core}'s real source
 * ({@code HadoopTableOperations.renameToFinal}), not assumed:
 * <ol>
 *   <li>It acquires a {@code LockManager} lock before committing — but
 *       {@code HadoopTables}' default {@code LockManager} is {@code
 *       InMemoryLockManager}, a plain {@code HashMap} living inside one
 *       JVM's memory (confirmed via bytecode). Separate processes each get
 *       their own private, empty lock table — this provides <i>zero</i>
 *       real protection across genuinely independent writers.</li>
 *   <li>It then checks {@code fs.exists(dst)} before committing — a
 *       classic check-then-act race with no real lock behind it: multiple
 *       writers can all see "doesn't exist yet" simultaneously.</li>
 *   <li>It then calls {@code fs.rename(src, dst)}, with the library's own
 *       comment stating <i>"this rename operation is the atomic commit
 *       operation"</i> — true on HDFS/local disk (a metadata-only pointer
 *       swap), false on S3A, where rename is copy-then-delete: two
 *       separate network calls, no compare-and-swap, no rejection if the
 *       destination was already written. Every racing writer's rename can
 *       return {@code true} (success) with no conflict ever detected —
 *       last writer to finish physically wins, silently, with no
 *       exception for anyone to retry on.</li>
 * </ol>
 * This matches Iceberg's own documentation, which already calls {@code
 * HadoopTables}/{@code HadoopCatalog} unsafe for concurrent use on
 * S3-compatible storage — the real fix is a catalog service (REST, Hive
 * Metastore, Glue) whose commit is backed by an actual database
 * transaction, not an object-store rename. No lock-based mitigation has
 * been built for this path (unlike {@code CommitWriter}'s {@code
 * LockProvider}) — Delta's log format needs one because every reader
 * depends on immutable numbered files; Iceberg's own documented answer is
 * "use a real catalog instead," which this project already supports via
 * {@link #IcebergTableWriter(Catalog, TableIdentifier)}, not yet
 * live-verified for concurrency either.
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
     *
     * <b>Confirmed broken for genuine concurrent writers on real MinIO
     * without a real lock configured</b> — see the class javadoc below
     * for the full root cause. {@link IcebergLockManagerConfig
     * #applyIfConfigured} opts this table into a real, cross-process
     * {@link DynamoDbLockManager} when {@code ICEBERG_LOCK_DYNAMODB_TABLE}
     * is set; a no-op otherwise, so the default behavior (single-writer
     * safe, confirmed-broken for concurrent writers) is unchanged.
     */
    public IcebergTableWriter(TableLocation location) {
        this.tableLocation = location;
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        IcebergLockManagerConfig.applyIfConfigured(conf);
        this.table = new HadoopTables(conf).load(location.raw());
    }

    /**
     * Catalog-service-based constructor — REST, Hive Metastore, Glue,
     * Nessie, or any other {@link Catalog} implementation. The catalog
     * only resolves *which table*; actually writing its data files still
     * goes through this project's existing {@code TableLocation}/Hadoop
     * {@code FileSystem}-based connectors, derived here from the loaded
     * table's own {@link Table#location()} via {@link
     * TableLocation#fromStringWithEnvironmentConfig} — so a remote
     * table's underlying storage (S3/MinIO/ADLS) still needs its own
     * connector env vars set (same as everywhere else in this project),
     * in addition to the catalog's own connection config.
     */
    public IcebergTableWriter(Catalog catalog, TableIdentifier identifier) {
        this.table = catalog.loadTable(identifier);
        this.tableLocation = TableLocation.fromStringWithEnvironmentConfig(table.location());
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
