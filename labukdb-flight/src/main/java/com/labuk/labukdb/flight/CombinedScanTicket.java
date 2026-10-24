package com.labuk.labukdb.flight;

import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.storage.TableLocation;

import java.util.List;
import java.util.Map;

/**
 * Everything {@code DoGet} needs to decode every surviving file, combine
 * them, and optionally aggregate/sort/limit — the counterpart to
 * {@link ScanTicket} for a query that can't be answered file-by-file
 * independently. One of these covers *all* surviving files for a query,
 * unlike {@link ScanTicket} which covers exactly one.
 */
final class CombinedScanTicket implements FlightTicket {

    /** One surviving file's scan parameters — the same three fields {@link ScanTicket} carries, bundled per-file here instead of one ticket each. */
    record FileScan(TableLocation dataFile, Predicate residualPredicate, Map<String, String> partitionValues) {
    }

    final List<FileScan> files;
    final List<String> projectedColumns;
    final List<String> groupByColumns;
    final List<AggregateSpec> aggregates;
    final List<SortKey> sortKeys;
    final long limit;

    CombinedScanTicket(List<FileScan> files, List<String> projectedColumns, List<String> groupByColumns,
                        List<AggregateSpec> aggregates, List<SortKey> sortKeys, long limit) {
        this.files = files;
        this.projectedColumns = projectedColumns;
        this.groupByColumns = groupByColumns;
        this.aggregates = aggregates;
        this.sortKeys = sortKeys;
        this.limit = limit;
    }
}
