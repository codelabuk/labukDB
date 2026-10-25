package com.labuk.labukdb.core.decode;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class DecimalCodecTest {

    @Test
    void toArrowType_buildsDecimal128WithGivenPrecisionAndScale() {
        ArrowType.Decimal type = DecimalCodec.toArrowType(10, 2);
        assertEquals(10, type.getPrecision());
        assertEquals(2, type.getScale());
        assertEquals(128, type.getBitWidth());
    }

    @Test
    void toArrowType_precisionOver38_rejectedWithClearError() {
        UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class,
                () -> DecimalCodec.toArrowType(39, 2));
        assertTrue(thrown.getMessage().contains("39"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("38"), thrown.getMessage());
    }

    @Test
    void fixedLengthBytes_roundTrips_positiveValue() {
        BigDecimal value = new BigDecimal("10.50");
        byte[] bytes = DecimalCodec.toFixedLengthBytes(value, 2);
        assertEquals(DecimalCodec.FIXED_LEN_BYTES, bytes.length);
        assertEquals(value, DecimalCodec.fromBytes(bytes, 2));
    }

    @Test
    void fixedLengthBytes_roundTrips_negativeValue() {
        BigDecimal value = new BigDecimal("-1234.56");
        byte[] bytes = DecimalCodec.toFixedLengthBytes(value, 2);
        assertEquals(DecimalCodec.FIXED_LEN_BYTES, bytes.length);
        assertEquals(value, DecimalCodec.fromBytes(bytes, 2));
    }

    @Test
    void fixedLengthBytes_roundTrips_zero() {
        BigDecimal value = new BigDecimal("0.00");
        byte[] bytes = DecimalCodec.toFixedLengthBytes(value, 2);
        assertEquals(value, DecimalCodec.fromBytes(bytes, 2));
    }

    @Test
    void fixedLengthBytes_roundTrips_maxPrecision38Value() {
        // 38 nines -- the largest unscaled value this codec claims to support.
        BigDecimal value = new BigDecimal(new BigInteger("9".repeat(38)), 0);
        byte[] bytes = DecimalCodec.toFixedLengthBytes(value, 0);
        assertEquals(value, DecimalCodec.fromBytes(bytes, 0));

        BigDecimal negative = value.negate();
        byte[] negativeBytes = DecimalCodec.toFixedLengthBytes(negative, 0);
        assertEquals(negative, DecimalCodec.fromBytes(negativeBytes, 0));
    }

    @Test
    void fixedLengthBytes_scaleMismatch_rejectedRatherThanSilentlyRounded() {
        // "10.567" (scale 3) forced to scale 2 would lose a digit -- must
        // reject rather than silently round, since an Arrow decimal
        // column's scale is fixed and a mismatch here means a caller bug
        // upstream, not something to paper over.
        BigDecimal value = new BigDecimal("10.567");
        assertThrows(ArithmeticException.class, () -> DecimalCodec.toFixedLengthBytes(value, 2));
    }

    @Test
    void fromUnscaledLong_appliesScaleCorrectly() {
        assertEquals(new BigDecimal("10.50"), DecimalCodec.fromUnscaledLong(1050L, 2));
        assertEquals(new BigDecimal("-10.50"), DecimalCodec.fromUnscaledLong(-1050L, 2));
        assertEquals(new BigDecimal("5"), DecimalCodec.fromUnscaledLong(5L, 0));
    }

    @Test
    void fromBytes_decodesVariableLengthBytes_notJustFixedLength16() {
        // Real-world files (e.g. Spark's INT32/INT64 or a shorter
        // FIXED_LEN_BYTE_ARRAY for small precision) don't use this
        // project's own 16-byte convention -- fromBytes must still decode
        // whatever width a real file actually used.
        BigInteger unscaled = BigInteger.valueOf(12345);
        byte[] fourBytes = unscaled.toByteArray();
        assertEquals(new BigDecimal(unscaled, 2), DecimalCodec.fromBytes(fourBytes, 2));
    }
}
