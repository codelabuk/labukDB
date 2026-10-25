package com.labuk.labukdb.core.decode;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;

/**
 * Parquet DECIMAL ↔ Arrow {@code Decimal}/{@link BigDecimal} — shared by
 * {@code decode/} and {@code encode/} (public for that reason; {@code
 * encode/} is already documented as "the write-side mirror of decode/",
 * so the dependency direction matches).
 *
 * <b>Decimal128 only, stated plainly — precision ≤38.</b> {@code
 * Decimal256Vector} (precision up to 76) is not supported; a precision
 * beyond 38 is rejected with a clear error by {@link #checkPrecisionSupported},
 * not silently mishandled or truncated.
 *
 * <b>Write narrow, read broad — same shape {@link Int96TimestampCodec}
 * already established for a different legacy-vs-modern split.</b> This
 * project's own writer ({@code encode.ArrowToParquetWriter}) always
 * encodes a decimal as a constant {@value #FIXED_LEN_BYTES}-byte {@code
 * FIXED_LEN_BYTE_ARRAY} regardless of actual precision — one encoding
 * path, always correct for any precision ≤38, trading a few
 * (compressible) wasted bytes for not needing the
 * minimal-bytes-for-precision formula real writers like Spark use. The
 * read side isn't limited to that convention: {@link #fromUnscaledLong}
 * and {@link #fromBytes} between them decode a decimal column backed by
 * any of Parquet's four legal physical representations (INT32, INT64,
 * FIXED_LEN_BYTE_ARRAY, BINARY) — real-world files (Spark-written, etc.)
 * use the precision-optimized choice, not this project's fixed-width one.
 */
public final class DecimalCodec {

    public static final int MAX_SUPPORTED_PRECISION = 38;
    public static final int FIXED_LEN_BYTES = 16;

    private DecimalCodec() {
    }

    public static void checkPrecisionSupported(int precision) {
        if (precision > MAX_SUPPORTED_PRECISION) {
            throw new UnsupportedOperationException(
                    "DECIMAL precision " + precision + " exceeds the supported maximum of "
                            + MAX_SUPPORTED_PRECISION + " (Decimal256 is not supported, only Decimal128)");
        }
    }

    public static ArrowType.Decimal toArrowType(int precision, int scale) {
        checkPrecisionSupported(precision);
        return new ArrowType.Decimal(precision, scale, 128);
    }

    /**
     * Big-endian two's-complement, sign-extended/padded to exactly {@link
     * #FIXED_LEN_BYTES} bytes — this project's own write-side convention.
     *
     * @throws IllegalArgumentException if {@code value}'s scale doesn't
     *                                   exactly match {@code scale} (an
     *                                   Arrow decimal column has one fixed
     *                                   scale for every value; a mismatch
     *                                   here would mean a caller bug
     *                                   upstream, not something to
     *                                   silently round), or if the
     *                                   unscaled value doesn't fit in
     *                                   {@value #FIXED_LEN_BYTES} bytes
     *                                   (shouldn't happen for ≤38 digits,
     *                                   checked rather than assumed).
     */
    public static byte[] toFixedLengthBytes(BigDecimal value, int scale) {
        BigDecimal rescaled = value.scale() == scale ? value : value.setScale(scale, RoundingMode.UNNECESSARY);
        byte[] raw = rescaled.unscaledValue().toByteArray();
        if (raw.length > FIXED_LEN_BYTES) {
            throw new IllegalArgumentException(
                    "Decimal value " + value + " does not fit in " + FIXED_LEN_BYTES + " bytes");
        }
        byte[] padded = new byte[FIXED_LEN_BYTES];
        byte signByte = raw.length > 0 && raw[0] < 0 ? (byte) 0xFF : 0x00;
        Arrays.fill(padded, signByte);
        System.arraycopy(raw, 0, padded, FIXED_LEN_BYTES - raw.length, raw.length);
        return padded;
    }

    /**
     * Decodes big-endian two's-complement bytes (any length — a real
     * file's {@code FIXED_LEN_BYTE_ARRAY} width depends on its own
     * writer's precision choice, and {@code BINARY}-backed decimals have
     * no fixed width at all) into a {@link BigDecimal} at the given scale.
     */
    public static BigDecimal fromBytes(byte[] bytes, int scale) {
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    /** For INT32/INT64-backed decimals — the physical value is the unscaled integer directly. */
    public static BigDecimal fromUnscaledLong(long unscaled, int scale) {
        return BigDecimal.valueOf(unscaled, scale);
    }
}
