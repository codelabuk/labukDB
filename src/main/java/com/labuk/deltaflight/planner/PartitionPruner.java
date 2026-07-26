package com.labuk.deltaflight.planner;

import com.labuk.deltaflight.log.action.AddFile;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Layer 2, step 1: eliminate whole files using their partition values, which
 * come straight from the commit log — no file is opened to do this pruning.
 *
 * A predicate on a column that isn't a partition column simply can't be
 * decided here (the file's actual values for that column are inside the
 * Parquet file), so this pruner defers — returns "might match" — for any
 * comparison whose column it doesn't recognize as a partition column,
 * leaving it to {@link FileStatsPruner} or the row-group/residual layers.
 */
public final class PartitionPruner {

    private PartitionPruner() {
    }

    public static List<AddFile> prune(Predicate predicate, Collection<AddFile> files) {
        if (predicate == null) {
            return List.copyOf(files);
        }
        return files.stream()
                .filter(f -> mightMatch(predicate, f.getPartitionValues()))
                .collect(Collectors.toList());
    }

    static boolean mightMatch(Predicate predicate, Map<String, String> partitionValues) {
        if (predicate instanceof Predicate.And and) {
            for (Predicate child : and.children) {
                if (!mightMatch(child, partitionValues)) {
                    return false;
                }
            }
            return true;
        }
        if (predicate instanceof Predicate.Or or) {
            for (Predicate child : or.children) {
                if (mightMatch(child, partitionValues)) {
                    return true;
                }
            }
            return false;
        }
        if (predicate instanceof Predicate.Comparison cmp) {
            if (!partitionValues.containsKey(cmp.column)) {
                return true; // not a partition column — defer to a later layer
            }
            return PredicateEvaluation.compareExact(partitionValues.get(cmp.column), cmp.op, cmp.literal);
        }
        return true; // unrecognized predicate shape — fail open
    }
}
