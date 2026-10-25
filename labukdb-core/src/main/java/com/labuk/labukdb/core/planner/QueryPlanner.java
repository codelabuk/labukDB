package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.format.TableFormat;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.action.AddFile;

import java.util.List;

/**
 * Entry point for Layer 2. Takes a resolved {@link Snapshot} plus what the
 * caller actually wants (a predicate, a column projection) and runs it
 * through the pruning stack in order, cheapest first:
 * partition values (log only) -&gt; file stats (log only) -&gt; [file opened
 * from here on, in decode/].
 *
 * <b>{@link #planNormalized} is the format-agnostic counterpart,</b> for
 * any {@link TableFormat}'s already-resolved {@link NormalizedSnapshot},
 * rather than Delta's own {@link Snapshot}/{@code AddFile} — today used
 * only by Iceberg's CLI/Flight query path, which resolves the snapshot
 * itself first (same {@code atVersion}/{@code atTimestamp}/{@code latest}
 * precedence Delta's own callers already use) so it can also report that
 * snapshot's version. {@link #plan} stays completely unchanged for Delta:
 * {@code Snapshot} is built from real commit-log actions in a way nothing
 * can fake for a table format that has no Delta commit log, so this is a
 * genuine second path, not a refactor of the first.
 */
public final class QueryPlanner {

    public ScanPlan plan(Snapshot snapshot, Predicate predicate, List<String> projectedColumns) {
        List<AddFile> allFiles = List.copyOf(snapshot.getActiveFiles());

        List<AddFile> survivors = PartitionPruner.prune(predicate, allFiles);
        survivors = FileStatsPruner.prune(predicate, survivors);

        return new ScanPlan(survivors, projectedColumns, predicate, allFiles.size());
    }

    public NormalizedScanPlan planNormalized(NormalizedSnapshot snapshot, Predicate predicate,
                                              List<String> projectedColumns) {
        List<DataFileRef> allFiles = List.copyOf(snapshot.getFiles());

        List<DataFileRef> survivors = PartitionPruner.pruneNormalized(predicate, allFiles);
        survivors = FileStatsPruner.pruneNormalized(predicate, survivors);

        return new NormalizedScanPlan(survivors, projectedColumns, predicate, allFiles.size());
    }
}
