package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.decode.ParquetToArrowReader;
import com.labuk.deltaflight.log.Snapshot;
import com.labuk.deltaflight.log.SnapshotManager;
import com.labuk.deltaflight.log.action.AddFile;
import com.labuk.deltaflight.planner.PartitionValueSubstituter;
import com.labuk.deltaflight.planner.Predicate;
import com.labuk.deltaflight.planner.QueryPlanner;
import com.labuk.deltaflight.planner.ScanPlan;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs a query directly through log/planner/decode, in-process — no Flight
 * server needed. This exercises exactly the same engine `DeltaFlightProducer`
 * calls, just without gRPC in between, which makes it useful both as a
 * genuinely usable query tool and as a fast way to isolate "is this a
 * server/network problem or an engine problem" when something misbehaves.
 *
 * Example:
 * <pre>
 *   java -cp target/classes:&lt;deps&gt; com.labuk.deltaflight.cli.DeltaCli \
 *     --table D:\repo\local-lakehouse-lab\lakehouse\logs_delta \
 *     --filter "level=ERROR" \
 *     --columns service,message \
 *     --limit 20
 * </pre>
 */
public final class DeltaCli {

    public static void main(String[] args) throws Exception {
        CliArgs parsed = CliArgs.parse(args);
        if (parsed == null) {
            CliArgs.printUsage();
            System.exit(1);
            return;
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Path tableRoot = Paths.get(parsed.tableRoot);
            SnapshotManager snapshotManager = new SnapshotManager(tableRoot);

            Snapshot snapshot = parsed.version != null ? snapshotManager.atVersion(parsed.version)
                    : parsed.asOf != null ? snapshotManager.atTimestamp(parsed.asOf)
                    : snapshotManager.latest();

            ScanPlan plan = new QueryPlanner().plan(snapshot, parsed.predicate, parsed.columns);

            System.out.println("Table version: " + snapshot.getVersion());
            System.out.println("Files: " + plan.getFilesToScan().size() + " / " + plan.getTotalFilesInSnapshot()
                    + " (skipped " + plan.getFilesSkipped() + " via partition/file-stats pruning)");
            System.out.println();

            ParquetToArrowReader reader = new ParquetToArrowReader();
            int totalRows = 0;
            boolean headerPrinted = false;
            int remaining = parsed.limit > 0 ? parsed.limit : Integer.MAX_VALUE;

            for (AddFile file : plan.getFilesToScan()) {
                if (remaining <= 0) {
                    break;
                }
                File parquetFile = tableRoot.resolve(file.getPath()).toFile();
                Predicate perFileResidual =
                        PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());

                try (VectorSchemaRoot root = reader.read(parquetFile, allocator, plan.getProjectedColumns(),
                        perFileResidual, file.getPartitionValues())) {
                    if (!headerPrinted) {
                        printHeader(root);
                        headerPrinted = true;
                    }
                    int toPrint = Math.min(root.getRowCount(), remaining);
                    printRows(root, toPrint);
                    totalRows += toPrint;
                    remaining -= toPrint;
                }
            }

            System.out.println();
            System.out.println(totalRows + " row(s)"
                    + (remaining <= 0 && parsed.limit > 0 ? " (hit --limit " + parsed.limit + ", more may exist)" : ""));
        }
    }

    private static void printHeader(VectorSchemaRoot root) {
        List<String> names = root.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        System.out.println(String.join(" | ", names));
        System.out.println("-".repeat(names.stream().mapToInt(String::length).sum() + 3 * names.size()));
    }

    private static void printRows(VectorSchemaRoot root, int count) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (int r = 0; r < count; r++) {
            List<String> cells = new ArrayList<>();
            for (FieldVector v : vectors) {
                Object value = v.getObject(r);
                cells.add(value == null ? "NULL" : value.toString());
            }
            System.out.println(String.join(" | ", cells));
        }
    }
}
