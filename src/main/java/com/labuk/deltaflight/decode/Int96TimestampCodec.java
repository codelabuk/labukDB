package com.labuk.deltaflight.decode;

import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * INT96 is Parquet's legacy timestamp encoding (superseded by INT64 with a
 * TIMESTAMP logical type annotation, but still what Spark writes by default
 * unless {@code spark.sql.parquet.outputTimestampType} is set otherwise).
 * Layout, 12 bytes total:
 *
 * <ul>
 *   <li>bytes 0-7: nanoseconds within the day, little-endian signed long</li>
 *   <li>bytes 8-11: Julian day number, little-endian signed int</li>
 * </ul>
 *
 * <b>Timezone caveat, stated plainly:</b> INT96 itself carries no timezone
 * information — it's just "this many nanoseconds into this Julian day."
 * This decoder treats that as UTC directly, with no rebasing. Spark's own
 * INT96 writer historically had a separate knob
 * ({@code spark.sql.parquet.int96TimestampConversion}) controlling whether
 * values got adjusted to/from the session-local timezone depending on
 * Spark version and configuration — a well-documented footgun in this exact
 * legacy format, and part of why Parquet moved to INT64-encoded
 * TIMESTAMP_MICROS/MILLIS with an explicit UTC-adjusted flag instead. If a
 * decoded timestamp is off by a fixed number of hours, that Spark
 * configuration — not this decoder — is the first thing to check.
 */
final class Int96TimestampCodec {

    // Julian Day Number of the Unix epoch (1970-01-01), the constant used
    // throughout the Hadoop ecosystem (Hive, Impala, Spark) for INT96
    // decode. Parquet itself doesn't standardize this; every INT96 reader
    // out there independently reimplements this same conversion.
    private static final long JULIAN_EPOCH_DAY = 2_440_588L;
    private static final long NANOS_PER_DAY = 86_400_000_000_000L;

    private Int96TimestampCodec() {
    }

    /** Returns epoch microseconds (UTC), truncating sub-microsecond precision. */
    static long toEpochMicros(Binary int96Value) {
        ByteBuffer buffer = ByteBuffer.wrap(int96Value.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buffer.getLong();
        int julianDay = buffer.getInt();

        long epochDay = julianDay - JULIAN_EPOCH_DAY;
        long epochNanos = epochDay * NANOS_PER_DAY + nanosOfDay;
        return Math.floorDiv(epochNanos, 1_000L);
    }

    /** Same decode as {@link #toEpochMicros}, as a java.time.Instant — the representation used for comparisons. */
    static java.time.Instant toInstant(Binary int96Value) {
        long micros = toEpochMicros(int96Value);
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanoAdjustment = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return java.time.Instant.ofEpochSecond(seconds, nanoAdjustment);
    }
}
