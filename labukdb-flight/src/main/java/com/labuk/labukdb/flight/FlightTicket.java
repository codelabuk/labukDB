package com.labuk.labukdb.flight;

/**
 * What {@link TicketRegistry} holds and {@code DoGet} dispatches on —
 * {@link ScanTicket} for the original one-file-per-endpoint scan path
 * (unchanged), {@link CombinedScanTicket} for a query needing
 * cross-file combination ({@code GROUP BY}/{@code ORDER BY}/{@code LIMIT}).
 */
sealed interface FlightTicket permits ScanTicket, CombinedScanTicket {
}
