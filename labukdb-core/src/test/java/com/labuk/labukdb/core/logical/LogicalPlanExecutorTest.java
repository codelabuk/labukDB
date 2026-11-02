package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.format.DeltaTableFormat;
import com.labuk.labukdb.core.query.SqlQuery;
import com.labuk.labukdb.core.query.SqlQueryTranslator;
import com.labuk.labukdb.core.storage.TableLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link LogicalPlanExecutor#resolve} against a real local Delta
 * fixture — the format-agnostic resolution step, exercised here for
 * Delta; the identical path for Iceberg is already proven end-to-end by
 * {@code QueryCommandIcebergIntegrationTest}/{@code
 * QueryCommandIcebergMinioIntegrationTest} in {@code labukdb-cli}, which
 * depends on both this module and {@code labukdb-format-iceberg} — a
 * dependency {@code labukdb-core}'s own test tree can't take.
 */
class LogicalPlanExecutorTest {

    private void writeFixtureCommit(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"logical-plan-executor-test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}",
                "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"ERROR\"},"
                        + "\"size\":12345,\"modificationTime\":1732000000500,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"level=INFO/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"INFO\"},"
                        + "\"size\":67890,\"modificationTime\":1732000000500,\"dataChange\":true}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    @Test
    void resolve_plainSelect_resolvesAllFilesNoCombine(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);
        SqlQuery query = SqlQueryTranslator.translate("SELECT amount FROM logs");
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

        ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, new DeltaTableFormat(tableRoot),
                TableLocation.forLocal(tableRoot), null, null);

        assertEquals(2, resolved.files().size(), "both partitions survive -- no WHERE clause to prune on");
        assertEquals(0, resolved.snapshotVersion());
        assertEquals(2, resolved.totalFilesInSnapshot());
        assertEquals(0, resolved.filesSkipped());
        assertFalse(resolved.needsCombine());
        assertNull(resolved.aggregate());
        assertNull(resolved.sort());
        assertNull(resolved.limit());
    }

    @Test
    void resolve_withPartitionPredicate_prunesToOneFile(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);
        SqlQuery query = SqlQueryTranslator.translate("SELECT amount FROM logs WHERE level = 'ERROR'");
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

        ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, new DeltaTableFormat(tableRoot),
                TableLocation.forLocal(tableRoot), null, null);

        assertEquals(1, resolved.files().size(), "partition pruning via the pushed-down predicate must skip the INFO file");
        assertEquals(1, resolved.filesSkipped());
    }

    @Test
    void resolve_groupByAggregate_needsCombineAndRequestsOnlyNeededColumns(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level");
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

        ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, new DeltaTableFormat(tableRoot),
                TableLocation.forLocal(tableRoot), null, null);

        assertTrue(resolved.needsCombine());
        assertNotNull(resolved.aggregate());
        assertEquals(List.of("level", "amount"), resolved.columns(),
                "needed columns = group-by column + aggregate input column, the same union QueryCommand computed inline before this rule existed");
    }

    @Test
    void resolve_atVersion_matchesRequestedVersion(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);
        SqlQuery query = SqlQueryTranslator.translate("SELECT amount FROM logs");
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

        ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, new DeltaTableFormat(tableRoot),
                TableLocation.forLocal(tableRoot), 0L, null);

        assertEquals(0, resolved.snapshotVersion());
    }
}
