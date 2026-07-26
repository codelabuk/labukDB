package com.labuk.deltaflight.decode;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Int96TimestampCodecTest {

    private static final long JULIAN_EPOCH_DAY = 2_440_588L;
    private static final long NANOS_PER_DAY = 86_400_000_000_000L;

    private static Binary encode(int julianDay, long nanosOfDay) {
        ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(nanosOfDay);
        buffer.putInt(julianDay);
        return Binary.fromConstantByteArray(buffer.array());
    }

    @Test
    void epochZero_decodesToZero() {
        // 1970-01-01T00:00:00Z is Julian day 2440588, midnight (0 nanos in).
        Binary encoded = encode((int) JULIAN_EPOCH_DAY, 0L);
        assertEquals(0L, Int96TimestampCodec.toEpochMicros(encoded));
    }

    @Test
    void roundTrip_arbitraryTimestamp() {
        long originalEpochMicros = 1_753_000_000_000_000L / 1000; // an arbitrary but fixed instant
        long epochNanos = originalEpochMicros * 1000L;
        long epochDay = Math.floorDiv(epochNanos, NANOS_PER_DAY);
        long nanosOfDay = epochNanos - epochDay * NANOS_PER_DAY;
        int julianDay = (int) (epochDay + JULIAN_EPOCH_DAY);

        Binary encoded = encode(julianDay, nanosOfDay);
        assertEquals(originalEpochMicros, Int96TimestampCodec.toEpochMicros(encoded));
    }

    @Test
    void roundTrip_dayBoundary() {
        // Exercises the floorDiv/mod arithmetic right at a day boundary,
        // where an off-by-one in the split between epochDay and nanosOfDay
        // would most likely surface.
        long originalEpochMicros = 86_400_000_000L; // exactly 1970-01-02T00:00:00Z, in micros
        long epochNanos = originalEpochMicros * 1000L;
        long epochDay = Math.floorDiv(epochNanos, NANOS_PER_DAY);
        long nanosOfDay = epochNanos - epochDay * NANOS_PER_DAY;
        int julianDay = (int) (epochDay + JULIAN_EPOCH_DAY);

        Binary encoded = encode(julianDay, nanosOfDay);
        assertEquals(originalEpochMicros, Int96TimestampCodec.toEpochMicros(encoded));
        assertEquals(0L, nanosOfDay, "exact midnight should have zero nanos-of-day");
    }
}
