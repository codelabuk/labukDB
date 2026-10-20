package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.format.TableFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TableFormat} for Iceberg — the second implementation
 * {@code format/TableFormat} was deliberately left unwired for, until one
 * existed to validate its shape against. Path-based only
 * ({@link HadoopTables#load}, no Hive/REST catalog service), read-only.
 *
 * <b>Two real shape mismatches this implementation surfaced — see
 * {@code docs/labukdb-v2-architecture.md} for the full writeup, not
 * silently worked around here:</b>
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
 *
 * Deliberately not wired into {@code planner}/{@code flight}/{@code cli}
 * yet — same as {@code DeltaTableFormat}, that migration happens once
 * both implementations have actually informed what the interface should
 * look like.
 */
public final class IcebergTableFormat implements TableFormat {

    private final Table table;

    public IcebergTableFormat(String tableLocation, Configuration conf) {
        this.table = new HadoopTables(conf).load(tableLocation);
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

    /** {@code "col1=val1/col2=val2"} -> {col1: val1, col2: val2}; "" (unpartitioned) -> empty map. */
    private static Map<String, String> parsePartitionPath(String partitionPath) {
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

    private List<String> partitionColumnNames() {
        List<String> names = new ArrayList<>();
        for (var field : table.spec().fields()) {
            names.add(field.name());
        }
        return names;
    }
}
