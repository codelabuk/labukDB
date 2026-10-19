package com.labuk.labukdb.flight;

import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.storage.TableLocation;

import java.util.List;
import java.util.Map;

/**
 * Everything {@code DoGet} needs to decode and stream exactly one file:
 * built once during {@code GetFlightInfo} planning, looked up once during
 * {@code DoGet} by the ticket id the client echoes back.
 *
 * Holds a {@link TableLocation} rather than a {@code java.io.File} so a
 * remote (S3/MinIO/ADLS) data file survives the round trip between
 * planning and fetching - carrying its Hadoop Configuration with it,
 * rather than being flattened into a local path that couldn't be reopened.
 */
final class ScanTicket {

    final TableLocation dataFile;
    final List<String> projectedColumns;
    final Predicate residualPredicate;
    final Map<String, String> partitionValues;

    ScanTicket(TableLocation dataFile, List<String> projectedColumns, Predicate residualPredicate,
               Map<String, String> partitionValues) {
        this.dataFile = dataFile;
        this.projectedColumns = projectedColumns;
        this.residualPredicate = residualPredicate;
        this.partitionValues = partitionValues;
    }
}
