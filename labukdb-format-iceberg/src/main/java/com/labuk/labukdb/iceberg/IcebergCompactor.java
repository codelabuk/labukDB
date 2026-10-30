package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.log.PartitionRowMerger;
import com.labuk.labukdb.core.storage.RemoteWriteSafetyWarning;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Merges every currently-active file for one table + partition into a
 * single new file — the Iceberg counterpart of {@code core.log.Compactor},
 * same scope and same deliberate omission (no size/age selection policy;
 * every call merges <em>all</em> matching files, regardless of whether
 * some are already large).
 *
 * <b>Commits via {@code Table.newRewrite()}, not {@code newOverwrite()}</b>
 * — confirmed via {@code javap} on {@code iceberg-api}: Iceberg has a
 * dedicated {@code RewriteFiles} commit type specifically for "this
 * doesn't change logical data, only its physical layout," distinct from
 * {@code OverwriteFiles} (used by {@link IcebergTableMerger}, where row
 * values genuinely change). This is the same {@code dataChange=false} vs.
 * {@code true} distinction Delta's {@code Compactor}/{@code TableMerger}
 * draw via {@code CommitWriter#replaceFiles}' own flag — getting this
 * wrong would make an incremental/CDC-style reader reprocess a pure
 * bin-packing commit as if real data had changed.
 *
 * <b>No retry loop, deliberately, matching Delta's {@code Compactor}'s own
 * reasoning stated plainly in its javadoc:</b> a bin-pack introduces no
 * new concurrency risk the old files' bytes stay on disk until {@code
 * Vacuum} removes them, so a reader resolved before this commit can still
 * read them, and a missed concurrent change just isn't compacted this
 * round, not a correctness problem. If {@code RewriteFiles.commit()}
 * itself throws on a genuine conflict (one of the files it's rewriting
 * was concurrently removed), that propagates to the caller unchanged,
 * exactly like Delta's version propagating {@code ConcurrentModificationException}.
 */
public final class IcebergCompactor {

    /**
     * Path-based ({@code HadoopTables}), local or remote.
     *
     * @return the new snapshot ID, or {@code -1} if fewer than 2 active
     *         files matched {@code partitionValues} — nothing meaningful
     *         to compact, not an error.
     */
    public long compact(TableLocation location, Map<String, String> partitionValues, BufferAllocator allocator) throws IOException {
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        boolean lockConfigured = IcebergLockManagerConfig.applyIfConfigured(conf);
        RemoteWriteSafetyWarning.warnIfUnsafe(location, lockConfigured, "Iceberg");
        Table table = new HadoopTables(conf).load(location.raw());
        return compactAgainstTable(table, location, partitionValues, allocator);
    }

    /** Catalog-based (REST, Hive Metastore, Glue, ...) — matches {@link IcebergTableWriter}'s own second constructor shape. */
    public long compact(Catalog catalog, TableIdentifier identifier, Map<String, String> partitionValues, BufferAllocator allocator) throws IOException {
        Table table = catalog.loadTable(identifier);
        TableLocation tableLocation = TableLocation.fromStringWithEnvironmentConfig(table.location());
        return compactAgainstTable(table, tableLocation, partitionValues, allocator);
    }

    private long compactAgainstTable(Table table, TableLocation tableLocation, Map<String, String> partitionValues,
                                      BufferAllocator allocator) throws IOException {
        table.refresh();
        List<DataFile> filesToCompact = IcebergTableFormat.filesInPartition(table, partitionValues);
        if (filesToCompact.size() < 2) {
            return -1;
        }

        List<TableLocation> fileLocations = filesToCompact.stream()
                .map(file -> IcebergDataFileWriter.locationOf(file.path().toString(), tableLocation))
                .toList();
        VectorSchemaRoot combined = PartitionRowMerger.combineFiles(fileLocations, allocator);
        try {
            DataFile newFile = IcebergDataFileWriter.write(table, tableLocation, partitionValues, combined);
            table.newRewrite()
                    .rewriteFiles(new HashSet<>(filesToCompact), Set.of(newFile))
                    .commit();
            return table.currentSnapshot().snapshotId();
        } finally {
            if (combined != null) {
                combined.close();
            }
        }
    }
}
