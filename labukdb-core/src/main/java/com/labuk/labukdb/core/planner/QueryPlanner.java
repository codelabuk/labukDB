package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.action.AddFile;

import java.util.List;

/**
 * Entry point for Layer 2. Takes a resolved {@link Snapshot} plus what the
 * caller actually wants (a predicate, a column projection) and runs it
 * through the pruning stack in order, cheapest first:
 * partition values (log only) -&gt; file stats (log only) -&gt; [file opened
 * from here on, in decode/].
 */
public final class QueryPlanner {

    public ScanPlan plan(Snapshot snapshot, Predicate predicate, List<String> projectedColumns) {
        List<AddFile> allFiles = List.copyOf(snapshot.getActiveFiles());

        List<AddFile> survivors = PartitionPruner.prune(predicate, allFiles);
        survivors = FileStatsPruner.prune(predicate, survivors);

        return new ScanPlan(survivors, projectedColumns, predicate, allFiles.size());
    }
}
