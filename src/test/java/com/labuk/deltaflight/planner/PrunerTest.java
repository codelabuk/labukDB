package com.labuk.deltaflight.planner;

import com.labuk.deltaflight.log.action.AddFile;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PrunerTest {

    // Builds an AddFile via reflection since its fields are private with no
    // public constructor (it's a Jackson deserialization target, not meant
    // to be hand-built in production code) — fine for a test-only fixture.
    private static AddFile addFile(String path, Map<String, String> partitionValues, String stats) throws Exception {
        AddFile file = new AddFile();
        setField(file, "path", path);
        setField(file, "partitionValues", partitionValues);
        setField(file, "stats", stats);
        return file;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = AddFile.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void partitionPruner_skipsNonMatchingPartition() throws Exception {
        AddFile errorFile = addFile("f1", Map.of("level", "ERROR"), null);
        AddFile infoFile = addFile("f2", Map.of("level", "INFO"), null);

        Predicate predicate = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        List<AddFile> survivors = PartitionPruner.prune(predicate, List.of(errorFile, infoFile));

        assertEquals(1, survivors.size());
        assertEquals("f1", survivors.get(0).getPath());
    }

    @Test
    void partitionPruner_defersOnNonPartitionColumn() throws Exception {
        AddFile file = addFile("f1", Map.of("level", "ERROR"), null);

        // "message" isn't a partition column — this pruner can't decide, must keep.
        Predicate predicate = Predicate.compare("message", ComparisonOp.EQ, "boom");
        List<AddFile> survivors = PartitionPruner.prune(predicate, List.of(file));

        assertEquals(1, survivors.size(), "non-partition-column predicates must not be pruned here");
    }

    @Test
    void partitionPruner_andRequiresAllChildrenToMatch() throws Exception {
        AddFile file = addFile("f1", Map.of("level", "ERROR", "region", "eu"), null);

        Predicate matching = Predicate.and(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("region", ComparisonOp.EQ, "eu"));
        Predicate nonMatching = Predicate.and(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("region", ComparisonOp.EQ, "us"));

        assertEquals(1, PartitionPruner.prune(matching, List.of(file)).size());
        assertEquals(0, PartitionPruner.prune(nonMatching, List.of(file)).size());
    }

    @Test
    void partitionPruner_orRequiresOnlyOneChildToMatch() throws Exception {
        AddFile file = addFile("f1", Map.of("level", "WARN"), null);

        Predicate predicate = Predicate.or(
                Predicate.compare("level", ComparisonOp.EQ, "ERROR"),
                Predicate.compare("level", ComparisonOp.EQ, "WARN"));

        assertEquals(1, PartitionPruner.prune(predicate, List.of(file)).size());
    }

    @Test
    void fileStatsPruner_skipsFileWhoseRangeCannotSatisfyPredicate() throws Exception {
        String stats = "{\"numRecords\":100,\"minValues\":{\"amount\":10},\"maxValues\":{\"amount\":50}}";
        AddFile file = addFile("f1", Map.of(), stats);

        // Predicate wants amount > 100 — file's max is 50, definitely no match.
        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 100L);
        assertEquals(0, FileStatsPruner.prune(predicate, List.of(file)).size());
    }

    @Test
    void fileStatsPruner_keepsFileWhoseRangeCouldSatisfyPredicate() throws Exception {
        String stats = "{\"numRecords\":100,\"minValues\":{\"amount\":10},\"maxValues\":{\"amount\":150}}";
        AddFile file = addFile("f1", Map.of(), stats);

        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 100L);
        assertEquals(1, FileStatsPruner.prune(predicate, List.of(file)).size());
    }

    @Test
    void fileStatsPruner_keepsFileWithNoStats() throws Exception {
        AddFile file = addFile("f1", Map.of(), null);

        Predicate predicate = Predicate.compare("amount", ComparisonOp.GT, 100L);
        assertEquals(1, FileStatsPruner.prune(predicate, List.of(file)).size(),
                "a file with no collected stats can't be pruned — it must be opened");
    }

    @Test
    void fileStatsPruner_keepsFileWhenColumnHasNoStatsEntry() throws Exception {
        String stats = "{\"numRecords\":100,\"minValues\":{\"amount\":10},\"maxValues\":{\"amount\":150}}";
        AddFile file = addFile("f1", Map.of(), stats);

        // "message" has no min/max recorded at all.
        Predicate predicate = Predicate.compare("message", ComparisonOp.EQ, "boom");
        assertEquals(1, FileStatsPruner.prune(predicate, List.of(file)).size());
    }

    @Test
    void neqNeverPrunesViaRange() {
        // Documented conservative behavior: NEQ never rules out a range.
        assertTrue(PredicateEvaluation.mightMatchRange(10L, 150L, ComparisonOp.NEQ, 10L));
    }
}
