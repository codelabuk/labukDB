package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.log.PartitionRowMerger;
import com.labuk.labukdb.core.storage.RemoteWriteSafetyWarning;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Upsert/MERGE by key — the Iceberg counterpart of {@code core.log.TableMerger},
 * reusing the exact same format-agnostic merge algorithm via {@link
 * PartitionRowMerger} (seed from what's already in the partition, apply
 * deletes then updates then inserts, last-applied-wins at the key level —
 * see that class's own javadoc for the precise per-row semantics, not
 * re-derived here). One table + one partition per call, same scope and
 * same deliberate omissions as Delta's version: keys stay within their
 * partition, no persistent key-column metadata, re-reads the whole
 * partition on every retry.
 *
 * <b>What's genuinely different from Delta's version, and why:</b>
 * <ul>
 *   <li>Commits via {@code Table.newOverwrite()} (delete the partition's
 *       current files, add one new merged file) instead of {@code
 *       CommitWriter#replaceFiles} — Iceberg's real atomic-replace
 *       primitive, confirmed via {@code javap} on {@code iceberg-api}.</li>
 *   <li>"Files currently in this partition" comes from a fresh {@code
 *       table.newScan().planFiles()} + the same partition-path parsing
 *       {@link IcebergTableFormat#parsePartitionPath} already uses for
 *       reads, not an {@code AddFile} list — Iceberg has no log-level
 *       partition index to query directly.</li>
 *   <li>The same staleness risk Delta's {@code stillFresh()} exists for
 *       (a concurrent writer adding a new file to this exact partition,
 *       which a plain "did my deleted files survive" check wouldn't
 *       catch) gets the identical hand-rolled check here, rather than
 *       leaning on {@code OverwriteFiles.validateNoConflictingData()} —
 *       that API's exact conflict semantics weren't independently
 *       verified against this precise scenario, so this mirrors Delta's
 *       own verified-correct approach instead of trusting an unverified
 *       one.</li>
 * </ul>
 *
 * <b>Path-based writes go through the same confirmed-broken-without-a-lock
 * gap {@code IcebergTableWriter} already documents</b> — {@code
 * OverwriteFiles} commits through the identical {@code HadoopTableOperations
 * .renameToFinal} rename-based path. {@link IcebergLockManagerConfig}/
 * {@link RemoteWriteSafetyWarning} apply here exactly as they do for
 * {@code IcebergTableWriter}.
 */
public final class IcebergTableMerger {

    private static final int MAX_RETRIES = 10;

    /** Path-based ({@code HadoopTables}), local or remote. */
    public long merge(TableLocation location, Map<String, String> partitionValues, List<String> keyColumns,
                       VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                       BufferAllocator allocator) throws IOException {
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        boolean lockConfigured = IcebergLockManagerConfig.applyIfConfigured(conf);
        RemoteWriteSafetyWarning.warnIfUnsafe(location, lockConfigured, "Iceberg");
        Table table = new HadoopTables(conf).load(location.raw());
        return mergeAgainstTable(table, location, partitionValues, keyColumns, inserts, updates, deletes, allocator);
    }

    /** Catalog-based (REST, Hive Metastore, Glue, ...) — matches {@link IcebergTableWriter}'s own second constructor shape. */
    public long merge(Catalog catalog, TableIdentifier identifier, Map<String, String> partitionValues, List<String> keyColumns,
                       VectorSchemaRoot inserts, VectorSchemaRoot updates, VectorSchemaRoot deletes,
                       BufferAllocator allocator) throws IOException {
        Table table = catalog.loadTable(identifier);
        TableLocation tableLocation = TableLocation.fromStringWithEnvironmentConfig(table.location());
        return mergeAgainstTable(table, tableLocation, partitionValues, keyColumns, inserts, updates, deletes, allocator);
    }

    private long mergeAgainstTable(Table table, TableLocation tableLocation, Map<String, String> partitionValues,
                                    List<String> keyColumns, VectorSchemaRoot inserts, VectorSchemaRoot updates,
                                    VectorSchemaRoot deletes, BufferAllocator allocator) throws IOException {
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("merge() requires at least one key column");
        }

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            table.refresh();
            List<DataFile> filesInPartition = filesInPartition(table, partitionValues);

            List<TableLocation> fileLocations = filesInPartition.stream()
                    .map(file -> dataFileLocation(file.path().toString(), tableLocation))
                    .toList();
            VectorSchemaRoot combined = PartitionRowMerger.combineFiles(fileLocations, allocator);
            VectorSchemaRoot merged;
            try {
                merged = PartitionRowMerger.merge(combined, keyColumns, inserts, updates, deletes, allocator);
            } finally {
                if (combined != null) {
                    combined.close();
                }
            }

            try {
                if (filesInPartition.isEmpty() && merged.getRowCount() == 0) {
                    return table.currentSnapshot() == null ? -1L : table.currentSnapshot().snapshotId();
                }
                if (!stillFresh(table, partitionValues, filesInPartition)) {
                    continue; // a concurrent writer touched this partition -- redo the whole merge against fresh data
                }

                OverwriteFiles overwrite = table.newOverwrite();
                for (DataFile file : filesInPartition) {
                    overwrite.deleteFile(file);
                }
                if (merged.getRowCount() > 0) {
                    DataFile newFile = IcebergDataFileWriter.write(table, tableLocation, partitionValues, merged);
                    overwrite.addFile(newFile);
                }
                overwrite.commit();
                return table.currentSnapshot().snapshotId();
            } catch (ValidationException | CommitFailedException conflict) {
                // another writer committed against this table since we started -- redo, same as Delta's retry loop
            } finally {
                merged.close();
            }
        }
        throw new IOException("Too much write contention on " + tableLocation
                + " -- merge failed to commit after " + MAX_RETRIES + " attempts");
    }

    /**
     * Catches what {@code OverwriteFiles}'s own built-in validation wasn't
     * independently verified to catch for this exact scenario: a
     * concurrent writer having ADDED a new file to this same partition,
     * not just removed one of ours — mirrors {@code TableMerger
     * #stillFresh}'s identical reasoning and identical fix.
     */
    private boolean stillFresh(Table table, Map<String, String> partitionValues, List<DataFile> expected) throws IOException {
        table.refresh();
        List<DataFile> current = filesInPartition(table, partitionValues);
        Set<String> currentPaths = current.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
        Set<String> expectedPaths = expected.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
        return currentPaths.equals(expectedPaths);
    }

    /**
     * A file's own spec (not the table's current one — partition spec can
     * evolve, same reasoning {@link IcebergTableFormat#toDataFileRef}
     * already applies), partition-path-parsed via {@link
     * IcebergTableFormat#parsePartitionPath}, matched against {@code
     * partitionValues} exactly the way {@code TableMerger} matches {@code
     * AddFile.getPartitionValues()}.
     */
    private List<DataFile> filesInPartition(Table table, Map<String, String> partitionValues) throws IOException {
        List<DataFile> result = new ArrayList<>();
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
            for (org.apache.iceberg.FileScanTask task : tasks) {
                DataFile file = task.file();
                PartitionSpec spec = table.specs().get(file.specId());
                String partitionPath = spec.partitionToPath(file.partition());
                if (IcebergTableFormat.parsePartitionPath(partitionPath).equals(partitionValues)) {
                    result.add(file);
                }
            }
        }
        return result;
    }

    private TableLocation dataFileLocation(String path, TableLocation tableLocation) {
        if (TableLocation.isRemoteUri(path)) {
            return TableLocation.forRemote(path, tableLocation.hadoopConf());
        }
        return TableLocation.forLocal(java.nio.file.Path.of(path));
    }
}
