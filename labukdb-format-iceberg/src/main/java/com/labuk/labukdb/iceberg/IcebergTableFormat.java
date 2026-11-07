package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.format.TableFormat;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TableFormat} for Iceberg — the second implementation that
 * validated {@code format/TableFormat}'s shape. Supports both path-based
 * ({@link HadoopTables#load}) and catalog-based access (including a REST
 * catalog, via {@code IcebergRestConfiguration}), and both reads and
 * writes (append, upsert/MERGE, compaction, vacuum — see the sibling
 * writer/merger/compactor/vacuum classes in this package).
 *
 * <b>Two real shape mismatches this implementation surfaced, worked
 * around rather than silently papered over:</b>
 * <ul>
 *   <li>{@link #atVersion}'s {@code long} parameter was shaped around
 *       Delta's sequential version numbers; here it's Iceberg's
 *       non-sequential {@code snapshotId} — type-compatible, not
 *       semantically the same "version N" callers might assume.</li>
 *   <li>{@link NormalizedSnapshot#getSchemaString()} is populated with
 *       Iceberg's {@code Schema.toString()} — readable, but not the same
 *       Delta-JSON-shaped string {@code DeltaTableFormat} produces. A
 *       caller treating this field as format-neutral today would be
 *       wrong to assume that.</li>
 * </ul>
 */
public final class IcebergTableFormat implements TableFormat {

    private final Table table;

    public IcebergTableFormat(String tableLocation, Configuration conf) {
        this.table = new HadoopTables(conf).load(tableLocation);
    }

    /**
     * {@code TableLocation}-based constructor — local or remote (S3/MinIO/
     * ADLS), the same dispatch every other {@code TableLocation}-aware
     * reader/writer in this project uses. A local location has no {@code
     * Configuration} of its own ({@link TableLocation#hadoopConf()} throws
     * for one); an empty default {@code Configuration} is sufficient for
     * local Hadoop {@code FileSystem} access, same as Hadoop's own default.
     */
    public IcebergTableFormat(TableLocation location) {
        this(location.raw(), location.isRemote() ? location.hadoopConf() : new Configuration());
    }

    /**
     * Catalog-service-based constructor — REST, Hive Metastore, Glue,
     * Nessie, or any other {@link Catalog} implementation, all sharing
     * this one interface. {@code normalize}/everything below is
     * unchanged from the path-based constructor; only how the {@link
     * Table} is obtained differs.
     */
    public IcebergTableFormat(Catalog catalog, TableIdentifier identifier) {
        this.table = catalog.loadTable(identifier);
    }

    /**
     * The table's own storage location, as Iceberg itself reports it —
     * for a caller that needs to resolve a {@link DataFileRef}'s
     * (already-absolute) path against something, without having supplied
     * a {@code TableLocation} directly (e.g. after loading via a {@link
     * Catalog}, where none was given).
     */
    public String location() {
        return table.location();
    }

    @Override
    public NormalizedSnapshot latest() {
        return normalize(table.newScan());
    }

    /** @param snapshotId Iceberg's snapshot ID — not a sequential index, see class javadoc. */
    @Override
    public NormalizedSnapshot atVersion(long snapshotId) {
        return normalize(table.newScan().useSnapshot(snapshotId));
    }

    @Override
    public NormalizedSnapshot atTimestamp(Instant timestamp) {
        return normalize(table.newScan().asOfTime(timestamp.toEpochMilli()));
    }

    private NormalizedSnapshot normalize(TableScan scan) {
        Snapshot snapshot = scan.snapshot();
        List<DataFileRef> files = new ArrayList<>();

        if (snapshot == null) {
            // A freshly created table (new IcebergTableCreator, never
            // appended to) has no current snapshot at all -- Iceberg's own
            // convention, genuinely different from Delta, where version 0
            // (metadata + protocol, no data) always exists right after
            // creation. scan.planFiles() would itself fail here, so this
            // must be checked before ever calling it, not caught after.
            // -1 mirrors Iceberg's own "no current snapshot" convention
            // (TableMetadata's currentSnapshotId == -1) rather than
            // inventing a different sentinel.
            return new NormalizedSnapshot(-1L, table.schema().toString(), partitionColumnNames(), files);
        }

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(toDataFileRef(task.file()));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("Failed to plan files for Iceberg table " + table.location(), e);
        }

        return new NormalizedSnapshot(
                snapshot.snapshotId(),
                table.schema().toString(),
                partitionColumnNames(),
                files);
    }

    private DataFileRef toDataFileRef(DataFile file) {
        // A file's own spec (file.specId()), not the table's current spec —
        // partition spec can evolve, and an older file's partition values
        // must be interpreted against the spec it was actually written
        // under, not whatever the table's latest spec happens to be.
        PartitionSpec spec = table.specs().get(file.specId());
        String partitionPath = spec.partitionToPath(file.partition());
        return new DataFileRef(
                file.path().toString(),
                parsePartitionPath(partitionPath),
                file.fileSizeInBytes(),
                null); // Iceberg's column-level stats aren't in AddFile.stats' JSON shape; not mapped here yet.
    }

    /**
     * {@code "col1=val1/col2=val2"} -> {col1: val1, col2: val2}; "" (unpartitioned) -> empty map.
     * Package-private (not {@code private}) so {@link IcebergTableMerger} can reuse the
     * identical partition-path parsing to filter a scan's files by partition.
     */
    static Map<String, String> parsePartitionPath(String partitionPath) {
        Map<String, String> values = new LinkedHashMap<>();
        if (partitionPath == null || partitionPath.isEmpty()) {
            return values;
        }
        for (String segment : partitionPath.split("/")) {
            int eq = segment.indexOf('=');
            if (eq > 0) {
                values.put(segment.substring(0, eq), segment.substring(eq + 1));
            }
        }
        return values;
    }

    /**
     * Every {@code DataFile} currently in {@code table} whose partition
     * values (resolved against its own spec — see {@link #toDataFileRef}'s
     * identical reasoning) equal {@code partitionValues}. Package-private,
     * shared by {@link IcebergTableMerger} and {@link IcebergCompactor} —
     * Iceberg has no log-level partition index to query directly the way
     * Delta's {@code AddFile} list provides, so both need this same scan.
     */
    static List<DataFile> filesInPartition(Table table, Map<String, String> partitionValues) throws java.io.IOException {
        List<DataFile> result = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                PartitionSpec spec = table.specs().get(file.specId());
                String partitionPath = spec.partitionToPath(file.partition());
                if (parsePartitionPath(partitionPath).equals(partitionValues)) {
                    result.add(file);
                }
            }
        }
        return result;
    }

    private List<String> partitionColumnNames() {
        List<String> names = new ArrayList<>();
        for (var field : table.spec().fields()) {
            names.add(field.name());
        }
        return names;
    }
}
