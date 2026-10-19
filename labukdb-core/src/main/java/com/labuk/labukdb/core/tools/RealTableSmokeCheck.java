package com.labuk.labukdb.core.tools;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.planner.ScanPlan;
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
 *   java -cp target/classes:&lt;deps&gt; com.labuk.labukdb.core.tools.RealTableSmokeCheck <tableRoot>
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
 *
 * <h2>Testing pruning and projection concretely against your real table</h2>
 * This tool now also runs two example queries through {@link QueryPlanner}
 * directly — no Flight server needed — so you can SEE the file counts
 * change rather than take "pruning works" on faith:
 * <ul>
 *   <li><b>Example A — partition-only filter:</b> {@code level = "ERROR"}.
 *       Watch "files after Layer 2 pruning" drop below the total active
 *       file count — every {@code level=INFO}/{@code level=WARN} folder's
 *       files get skipped without ever being opened, regardless of which
 *       {@code log_date} they're under.</li>
 *   <li><b>Example B — full scan (no predicate):</b> for comparison, shows
 *       every active file surviving, confirming Example A's reduction is
 *       real pruning and not just an empty table.</li>
 * </ul>
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
            System.out.println("No active files — nothing further to check.");
            return;
        }

        AddFile firstFile = files.iterator().next();
        System.out.println("Sampling schema from: " + firstFile.getPath());
        File parquetFile = tableRoot.resolve(firstFile.getPath()).toFile();
        Schema schema = new ParquetToArrowReader().readSchema(parquetFile, null);
        System.out.println("Inferred Arrow schema:");
        schema.getFields().forEach(f -> System.out.println("  " + f.getName() + " : " + f.getType()));

        System.out.println();
        System.out.println("--- Example A: partition-only pruning, level = \"ERROR\" ---");
        QueryPlanner planner = new QueryPlanner();
        Predicate levelFilter = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        ScanPlan planA = planner.plan(snapshot, levelFilter, null);
        System.out.println("Total active files: " + planA.getTotalFilesInSnapshot());
        System.out.println("Files after Layer 2 pruning: " + planA.getFilesToScan().size());
        System.out.println("Files skipped (never opened): " + planA.getFilesSkipped());

        System.out.println();
        System.out.println("--- Example B: no predicate, for comparison ---");
        ScanPlan planB = planner.plan(snapshot, null, null);
        System.out.println("Files after Layer 2 pruning (should equal total): " + planB.getFilesToScan().size());

        System.out.println();
        System.out.println("If Example A's file count is smaller than Example B's, partition "
                + "pruning is genuinely reducing what gets opened on your real table, not just in fixtures.");
    }
}
