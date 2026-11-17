package com.labuk.labukdb.core.encode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link FileStatsCollector} produces real Delta-shaped file statistics —
 * see its own javadoc for the exact shape confirmed against a genuine
 * Spark-produced commit this session. These tests prove the math (min/max/
 * null-count per column) and the one subtlety easy to get wrong: a
 * timestamp column's min/max must come out as an ISO-8601 string, not a
 * raw epoch-micros long, or {@code PredicateEvaluation.coerce} would never
 * match it against a predicate's {@code Instant} literal.
 */
class FileStatsCollectorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void numericAndStringColumns_minMaxAndNullCountAllCorrect() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(amount, service)), allocator)) {
                batch.allocateNew();
                BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
                VarCharVector serviceVector = (VarCharVector) batch.getVector("service");

                amountVector.setSafe(0, 100);
                serviceVector.setSafe(0, "auth-svc".getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(1, 300);
                serviceVector.setNull(1);
                amountVector.setNull(2);
                serviceVector.setSafe(2, "payments-svc".getBytes(StandardCharsets.UTF_8));
                batch.setRowCount(3);

                JsonNode stats = mapper.readTree(FileStatsCollector.collect(batch));
                assertEquals(3, stats.get("numRecords").asInt());
                assertEquals(100, stats.get("minValues").get("amount").asLong());
                assertEquals(300, stats.get("maxValues").get("amount").asLong());
                assertEquals("auth-svc", stats.get("minValues").get("service").asText());
                assertEquals("payments-svc", stats.get("maxValues").get("service").asText());
                assertEquals(1, stats.get("nullCount").get("amount").asLong());
                assertEquals(1, stats.get("nullCount").get("service").asLong());
            }
        }
    }

    @Test
    void booleanColumn_minMaxCorrect() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field flag = new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(flag)), allocator)) {
                batch.allocateNew();
                BitVector flagVector = (BitVector) batch.getVector("flag");
                flagVector.setSafe(0, 1);
                flagVector.setSafe(1, 0);
                flagVector.setSafe(2, 1);
                batch.setRowCount(3);

                JsonNode stats = mapper.readTree(FileStatsCollector.collect(batch));
                assertFalse(stats.get("minValues").get("flag").asBoolean());
                assertTrue(stats.get("maxValues").get("flag").asBoolean());
            }
        }
    }

    @Test
    void timestampColumn_minMaxAreIso8601Strings_notRawEpochMicros() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field ts = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(ts)), allocator)) {
                batch.allocateNew();
                TimeStampMicroVector tsVector = (TimeStampMicroVector) batch.getVector("ts");
                Instant earlier = Instant.parse("2026-01-01T00:00:00Z");
                Instant later = Instant.parse("2026-06-15T12:30:00Z");
                tsVector.setSafe(0, earlier.getEpochSecond() * 1_000_000L + earlier.getNano() / 1000);
                tsVector.setSafe(1, later.getEpochSecond() * 1_000_000L + later.getNano() / 1000);
                batch.setRowCount(2);

                JsonNode stats = mapper.readTree(FileStatsCollector.collect(batch));
                String min = stats.get("minValues").get("ts").asText();
                String max = stats.get("maxValues").get("ts").asText();
                assertEquals(earlier, Instant.parse(min), "min must round-trip through Instant.parse, not be a raw long: " + min);
                assertEquals(later, Instant.parse(max), "max must round-trip through Instant.parse, not be a raw long: " + max);
            }
        }
    }

    @Test
    void decimalColumn_noMinMax_butNullCountStillCollected() throws Exception {
        // Named gap, stated plainly in FileStatsCollector's own javadoc: no
        // min/max for DecimalVector -- FileStatsPruner already treats a
        // missing min/max as "keep the file," so this is a safe omission,
        // not silently wrong. nullCount needs no type-specific logic at all,
        // so it's still collected correctly even for an unsupported type.
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field price = new Field("price", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(price)), allocator)) {
                batch.allocateNew();
                DecimalVector priceVector = (DecimalVector) batch.getVector("price");
                priceVector.setSafe(0, java.math.BigDecimal.valueOf(12.34));
                priceVector.setNull(1);
                batch.setRowCount(2);

                JsonNode stats = mapper.readTree(FileStatsCollector.collect(batch));
                assertFalse(stats.get("minValues").has("price"), "no min/max collected for DecimalVector");
                assertFalse(stats.get("maxValues").has("price"));
                assertEquals(1, stats.get("nullCount").get("price").asLong());
            }
        }
    }

    @Test
    void allNullColumn_noMinMaxEntry_justNullCount() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(amount)), allocator)) {
                batch.allocateNew();
                BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
                amountVector.setNull(0);
                amountVector.setNull(1);
                batch.setRowCount(2);

                JsonNode stats = mapper.readTree(FileStatsCollector.collect(batch));
                assertFalse(stats.get("minValues").has("amount"), "an all-null column has no min/max to report");
                assertFalse(stats.get("maxValues").has("amount"));
                assertEquals(2, stats.get("nullCount").get("amount").asLong());
            }
        }
    }
}
