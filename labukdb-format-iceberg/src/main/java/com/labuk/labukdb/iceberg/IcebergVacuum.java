package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.log.Vacuum;
import com.labuk.labukdb.core.storage.RemoteWriteSafetyWarning;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Physically deletes files Iceberg's own metadata has already expired —
 * the Iceberg counterpart of {@code core.log.Vacuum}, but deliberately
 * much thinner: unlike Delta's hand-rolled version (scanning commit
 * history for old {@code remove} actions, checking each against the
 * live set itself), Iceberg already ships this as a first-class,
 * {@code iceberg-core}-level operation — {@code Table.expireSnapshots()}
 * — not a Spark-only action. This class is a thin wrapper supplying this
 * project's own file-deletion primitive (local {@code Files
 * .deleteIfExists} / remote Hadoop {@code FileSystem.delete} — the exact
 * same two-branch logic {@link Vacuum#vacuum} already uses) and
 * capturing which paths were actually removed, matching {@code Vacuum
 * .vacuum}'s own {@code List<String>} return contract.
 *
 * <b>{@code deleteWith(Consumer<String>)} REPLACES the default delete
 * implementation, not a side-channel notification on top of one</b> —
 * confirmed against Iceberg's own source and javadoc, not assumed: if
 * this weren't supplied, Iceberg would delete expired files itself using
 * its own {@code FileIO}; supplying it means *this* class's consumer is
 * the only thing that actually removes bytes.
 *
 * <b>{@code retainLast(1)}</b> mirrors the one safety invariant Delta's
 * {@code Vacuum} javadoc calls out explicitly: a file still needed by
 * the live table is never deleted, regardless of age — Iceberg's own
 * equivalent is "always keep at least the current snapshot."
 *
 * <b>A real format difference, not silently papered over:</b> the
 * returned paths aren't limited to Parquet data files the way Delta's
 * {@code Vacuum} always is — Iceberg's expiration also deletes stale
 * manifest and manifest-list files, which have no Delta equivalent
 * (Delta's log is flat JSON/checkpoint files, never separately vacuumed
 * by this mechanism). Callers expecting "only data files" would be
 * surprised by this list containing manifest paths too.
 *
 * <b>Another real subtlety, confirmed by tracing {@code
 * IncrementalFileCleanup}'s actual source, not assumed:</b> a removed
 * data file only becomes physically deletable once the snapshot that
 * <em>recorded its removal</em> (e.g. a compaction or merge commit) is
 * itself expired — not when the snapshot that originally added the file
 * expires. If that removing commit is still the table's current
 * snapshot (protected by {@code retainLast}), the file it removed stays
 * on disk indefinitely regardless of age. In practice this just means a
 * table needs at least one commit after a compaction/merge before that
 * operation's removed files become eligible — the same ordering {@code
 * IcebergVacuumTest} proves against a real compaction, not a synthetic
 * fixture.
 */
public final class IcebergVacuum {

    /** Matches {@code core.log.Vacuum.DEFAULT_RETENTION} — same real Delta Lake default, kept consistent across formats. */
    public static final Duration DEFAULT_RETENTION = Vacuum.DEFAULT_RETENTION;

    public List<String> vacuum(TableLocation location) throws IOException {
        return vacuum(location, DEFAULT_RETENTION);
    }

    /** Path-based ({@code HadoopTables}), local or remote. @return the paths actually deleted. */
    public List<String> vacuum(TableLocation location, Duration retentionPeriod) throws IOException {
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        boolean lockConfigured = IcebergLockManagerConfig.applyIfConfigured(conf);
        // Vacuum only deletes files an earlier commit already tombstoned --
        // single-writer-only is this path's documented contract, same as
        // IcebergTableWriter's own path-based ctor, when no lock env var is set.
        RemoteWriteSafetyWarning.enforce(location, lockConfigured, true, "Iceberg");
        Table table = new HadoopTables(conf).load(location.raw());
        return vacuumAgainstTable(table, location, retentionPeriod);
    }

    public List<String> vacuum(Catalog catalog, TableIdentifier identifier) throws IOException {
        return vacuum(catalog, identifier, DEFAULT_RETENTION);
    }

    /** Catalog-based (REST, Hive Metastore, Glue, ...) — matches {@link IcebergTableWriter}'s own second constructor shape. */
    public List<String> vacuum(Catalog catalog, TableIdentifier identifier, Duration retentionPeriod) throws IOException {
        Table table = catalog.loadTable(identifier);
        TableLocation tableLocation = TableLocation.fromStringWithEnvironmentConfig(table.location());
        return vacuumAgainstTable(table, tableLocation, retentionPeriod);
    }

    private List<String> vacuumAgainstTable(Table table, TableLocation tableLocation, Duration retentionPeriod) {
        long cutoffMillis = System.currentTimeMillis() - retentionPeriod.toMillis();
        List<String> deleted = new ArrayList<>();
        table.expireSnapshots()
                .expireOlderThan(cutoffMillis)
                .retainLast(1)
                // ALL is already the real default (confirmed by reading
                // RemoveSnapshots' actual 1.12.0 source, not assumed) --
                // set explicitly anyway so this class's own contract
                // (always reclaim data file bytes, matching Delta's
                // Vacuum) doesn't silently change if a future Iceberg
                // version ever changes that default.
                .cleanupLevel(org.apache.iceberg.ExpireSnapshots.CleanupLevel.ALL)
                .deleteWith(path -> {
                    if (deleteDataFile(IcebergDataFileWriter.locationOf(path, tableLocation))) {
                        deleted.add(path);
                    }
                })
                .commit();
        return deleted;
    }

    /**
     * {@code Consumer<String>} allows no checked exceptions -- wraps as
     * {@link UncheckedIOException}, the same idiom {@code IcebergTableFormat
     * .normalize} already uses elsewhere in this module.
     */
    private boolean deleteDataFile(TableLocation dataFile) {
        try {
            if (!dataFile.isRemote()) {
                return Files.deleteIfExists(dataFile.localPath());
            }
            return dataFile.hadoopPath().getFileSystem(dataFile.hadoopConf()).delete(dataFile.hadoopPath(), false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
