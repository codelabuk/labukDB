package com.labuk.labukdb.flight.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.planner.Predicate;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class PredicateCodecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonNode json(String s) throws Exception {
        return MAPPER.readTree(s);
    }

    @Test
    void decodesInPredicate() throws Exception {
        Predicate predicate = PredicateCodec.fromJson(json(
                "{\"type\":\"in\",\"column\":\"level\",\"literals\":[\"ERROR\",\"WARN\"],\"literalType\":\"string\"}"));

        assertTrue(predicate instanceof Predicate.In);
        Predicate.In in = (Predicate.In) predicate;
        assertEquals("level", in.column);
        assertEquals(2, in.literals.size());
        assertEquals("ERROR", in.literals.get(0));
    }

    @Test
    void decodesLikePredicate() throws Exception {
        Predicate predicate = PredicateCodec.fromJson(json(
                "{\"type\":\"like\",\"column\":\"log_date\",\"pattern\":\"2026-07%\"}"));

        assertTrue(predicate instanceof Predicate.Like);
        assertEquals("2026-07%", ((Predicate.Like) predicate).pattern);
    }

    @Test
    void decodesTimestampLiteral_asInstant() throws Exception {
        Predicate predicate = PredicateCodec.fromJson(json(
                "{\"type\":\"cmp\",\"column\":\"event_ts\",\"op\":\"GT\","
                        + "\"literal\":\"2026-07-17T10:00:00Z\",\"literalType\":\"timestamp\"}"));

        Predicate.Comparison cmp = (Predicate.Comparison) predicate;
        assertEquals(Instant.parse("2026-07-17T10:00:00Z"), cmp.literal);
    }

    @Test
    void inWithoutLiteralsArray_throws() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                PredicateCodec.fromJson(json("{\"type\":\"in\",\"column\":\"level\"}")));
    }
}
