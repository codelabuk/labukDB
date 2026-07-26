package com.labuk.deltaflight.flight;

import com.labuk.deltaflight.planner.Predicate;

import java.io.File;
import java.util.List;

/**
 * Everything {@code DoGet} needs to decode and stream exactly one file:
 * built once during {@code GetFlightInfo} planning, looked up once during
 * {@code DoGet} by the ticket id the client echoes back.
 */
final class ScanTicket {

    final File parquetFile;
    final List<String> projectedColumns;
    final Predicate residualPredicate;

    ScanTicket(File parquetFile, List<String> projectedColumns, Predicate residualPredicate) {
        this.parquetFile = parquetFile;
        this.projectedColumns = projectedColumns;
        this.residualPredicate = residualPredicate;
    }
}
