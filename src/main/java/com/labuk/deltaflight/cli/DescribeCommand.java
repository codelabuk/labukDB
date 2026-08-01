package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.catalog.TableCatalog;
import com.labuk.deltaflight.decode.ParquetToArrowReader;
import com.labuk.deltaflight.log.Snapshot;
import com.labuk.deltaflight.log.SnapshotManager;
import com.labuk.deltaflight.log.action.AddFile;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;

/** {@code delta-cli describe <name-or-path>} — schema and table metadata, no row data touched. */
final class DescribeCommand {

    private DescribeCommand() {
    }

    static void run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: delta-cli describe <name-or-path>");
            System.exit(1);
            return;
        }

        TableCatalog catalog = TableCatalog.atDefaultLocation();
        Path tableRoot = TableResolver.resolve(args[0], catalog);

        SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
        Snapshot snapshot = snapshotManager.latest();

        System.out.println("Table: " + tableRoot);
        System.out.println("Version: " + snapshot.getVersion());
        System.out.println("Active files: " + snapshot.getActiveFileCount());
        System.out.println("Partition columns: "
                + (snapshot.getMetadata() != null ? snapshot.getMetadata().getPartitionColumns() : "(none found)"));

        Collection<AddFile> files = snapshot.getActiveFiles();
        if (files.isEmpty()) {
            System.out.println("No active files — nothing further to describe.");
            return;
        }

        AddFile sample = files.iterator().next();
        Schema schema = new ParquetToArrowReader().readSchema(
                new File(tableRoot.resolve(sample.getPath()).toString()), null, sample.getPartitionValues());

        System.out.println();
        System.out.println("Schema (physical + partition columns):");
        schema.getFields().forEach(f -> System.out.println("  " + f.getName() + " : " + f.getType()));
    }
}
