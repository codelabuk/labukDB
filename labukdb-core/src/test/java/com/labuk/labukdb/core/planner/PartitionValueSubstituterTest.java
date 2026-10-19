package com.labuk.labukdb.core.planner;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PartitionValueSubstituterTest {

    @Test
    void purePartitionPredicate_foldsToNull_meaningNoFilterNeeded() {
        Predicate predicate = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        Predicate simplified = PartitionValueSubstituter.simplify(predicate, Map.of("level", "ERROR"));

        assertNull(simplified, "a predicate fully satisfied by this file's partition value needs no residual check");
    }

    @Test
    void mixedAnd_dropsThePartitionClauseAndKeepsThePhysicalOne() {
        Predicate predicate = Predicate.and(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("amount", ComparisonOp.GT, 2L));

        Predicate simplified = PartitionValueSubstituter.simplify(predicate, Map.of("level", "ERROR"));

        assertNotNull(simplified);
        assertTrue(simplified instanceof Predicate.Comparison,
                "after dropping the satisfied partition clause, only the physical-column comparison should remain");
        assertEquals("amount", ((Predicate.Comparison) simplified).column);
    }

    @Test
    void mixedOr_partitionClauseTrue_wholeExpressionBecomesUnfiltered() {
        Predicate predicate = Predicate.or(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("amount", ComparisonOp.GT, 999L));

        Predicate simplified = PartitionValueSubstituter.simplify(predicate, Map.of("level", "ERROR"));

        assertNull(simplified, "one TRUE branch of an OR makes the whole predicate TRUE for this file");
    }

    @Test
    void multiplePartitionColumns_allFoldAway() {
        Predicate predicate = Predicate.and(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("log_date", ComparisonOp.EQ, "2026-07-17"));

        Predicate simplified = PartitionValueSubstituter.simplify(
                predicate, Map.of("level", "ERROR", "log_date", "2026-07-17"));

        assertNull(simplified);
    }

    @Test
    void nonPartitionPredicate_isUntouched() {
        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 2L);
        Predicate simplified = PartitionValueSubstituter.simplify(predicate, Map.of("level", "ERROR"));

        assertSame(predicate, simplified, "a predicate with no partition-column clauses should pass through unchanged");
    }

    @Test
    void nullPredicate_staysNull() {
        assertNull(PartitionValueSubstituter.simplify(null, Map.of("level", "ERROR")));
    }
}
