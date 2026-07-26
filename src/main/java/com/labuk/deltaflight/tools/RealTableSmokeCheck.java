package com.labuk.deltaflight.tools;

import com.labuk.deltaflight.decode.ParquetToArrowReader;
import com.labuk.deltaflight.log.Snapshot;
import com.labuk.deltaflight.log.SnapshotManager;
import com.labuk.deltaflight.log.action.AddFile;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

/**
 * Run this against a real table before wiring up the Flight server —
 * it isolates Layer 1 (log/snapshot) and the schema-inference part of
 * Layer 3, without Flight or a running server in the picture at all.
 *
 * Usage:
 *   java -cp target/classes:&lt;deps&gt; com.labuk.deltaflight.tools.RealTableSmokeCheck <tableRoot>
 *
 * e.g. against local-lakehouse-lab's streaming output:
 *   ... RealTableSmokeCheck D:\repo\local-lakehouse-lab\lakehouse\logs_delta
 *
 * What a clean run looks like: snapshot resolves, active file count and
 * partition columns print, and the first file's schema prints without an
 * UnsupportedOperationException. If it throws instead, the exception
 * message tells you exactly which layer and which assumption broke —
 * that's the point of running this before the Flight server, which would
 * otherwise bury the same failure inside a getFlightInfo gRPC error.
 */
public final class RealTableSmokeCheck {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: RealTableSmokeCheck <tableRoot>");
            System.exit(1);
        }

        Path tableRoot = Paths.get(args[0]);
        System.out.println("Table root: " + tableRoot.toAbsolutePath());

        SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
        Snapshot snapshot = snapshotManager.latest();

        System.out.println("Resolved version: " + snapshot.getVersion());
        System.out.println("Active file count: " + snapshot.getActiveFileCount());
        System.out.println("Partition columns: "
                + (snapshot.getMetadata() != null ? snapshot.getMetadata().getPartitionColumns() : "(no metaData found)"));

        Collection<AddFile> files = snapshot.getActiveFiles();
        if (files.isEmpty()) {
            System.out.println("No active files — nothing to schema-check.");
            return;
        }

        AddFile firstFile = files.iterator().next();
        System.out.println("Sampling schema from: " + firstFile.getPath());
        System.out.println("Sample file's partition values: " + firstFile.getPartitionValues());

        File parquetFile = tableRoot.resolve(firstFile.getPath()).toFile();
        Schema schema = new ParquetToArrowReader().readSchema(parquetFile, null);

        System.out.println("Inferred Arrow schema:");
        schema.getFields().forEach(f -> System.out.println("  " + f.getName() + " : " + f.getType()));

        System.out.println();
        System.out.println("If you got here without an exception, Layer 1 and the "
                + "schema-inference path in Layer 3 both handle this table's real shape.");
    }
}
