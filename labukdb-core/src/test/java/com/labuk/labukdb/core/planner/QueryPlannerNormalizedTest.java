package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link QueryPlanner#planNormalized} prunes {@link DataFileRef}s
 * exactly the way {@link QueryPlanner#plan} already prunes {@code AddFile}s
 * — same underlying {@code mightMatch} logic in {@link PartitionPruner}/
 * {@link FileStatsPruner}, reused via their new {@code pruneNormalized}
 * overloads, not reimplemented. Today's one real caller is Iceberg's
 * CLI/Flight query path; these fixtures are plain {@link DataFileRef}s,
 * not a real Iceberg table, since the pruning logic itself doesn't care
 * which format produced the file list.
 */
class QueryPlannerNormalizedTest {

    @Test
    void partitionPredicate_prunesByPartitionValue() {
        DataFileRef errorFile = new DataFileRef("level=ERROR/f1.parquet", Map.of("level", "ERROR"), 100, null);
        DataFileRef infoFile = new DataFileRef("level=INFO/f2.parquet", Map.of("level", "INFO"), 100, null);
        NormalizedSnapshot snapshot = new NormalizedSnapshot(1, null, List.of("level"), List.of(errorFile, infoFile));

        Predicate predicate = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, predicate, List.of());

        assertEquals(List.of(errorFile), plan.getFilesToScan());
        assertEquals(2, plan.getTotalFilesInSnapshot());
        assertEquals(1, plan.getFilesSkipped());
    }

    @Test
    void statsPredicate_prunesByOutOfRangeFile() {
        // amount in [1,5]
        DataFileRef lowFile = new DataFileRef("f1.parquet", Map.of(), 100,
                "{\"minValues\":{\"amount\":1},\"maxValues\":{\"amount\":5}}");
        // amount in [100,104] -- out of range for amount > 50
        DataFileRef highFile = new DataFileRef("f2.parquet", Map.of(), 100,
                "{\"minValues\":{\"amount\":100},\"maxValues\":{\"amount\":104}}");
        NormalizedSnapshot snapshot = new NormalizedSnapshot(1, null, List.of(), List.of(lowFile, highFile));

        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 50L);
        NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, predicate, List.of());

        assertEquals(List.of(highFile), plan.getFilesToScan());
    }

    @Test
    void nullStats_failsOpen_notPruned() {
        // Iceberg's DataFileRef.getStats() is always null today (its column
        // stats aren't mapped into this JSON shape yet) -- confirms that
        // doesn't silently drop files, just skips the stats-pruning benefit.
        DataFileRef file = new DataFileRef("f1.parquet", Map.of(), 100, null);
        NormalizedSnapshot snapshot = new NormalizedSnapshot(1, null, List.of(), List.of(file));

        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 50L);
        NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, predicate, List.of());

        assertEquals(List.of(file), plan.getFilesToScan());
    }

    @Test
    void nullPredicate_keepsEveryFile() {
        DataFileRef f1 = new DataFileRef("f1.parquet", Map.of(), 100, null);
        DataFileRef f2 = new DataFileRef("f2.parquet", Map.of(), 100, null);
        NormalizedSnapshot snapshot = new NormalizedSnapshot(1, null, List.of(), List.of(f1, f2));

        NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, null, List.of());

        assertEquals(List.of(f1, f2), plan.getFilesToScan());
        assertEquals(0, plan.getFilesSkipped());
    }
}
