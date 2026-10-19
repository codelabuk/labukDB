package com.labuk.labukdb.core.format;

import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link DeltaTableFormat} is a faithful, lossless translation of
 * what {@code SnapshotManager} already resolves — the actual bar for
 * "this interface's shape is plausible," not a claim that it's wired into
 * the rest of the engine yet (it isn't — see {@link TableFormat}'s
 * javadoc).
 */
class DeltaTableFormatTest {

    private void writeFixtureCommit(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"table-format-test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}",
                "{\"add\":{\"path\":\"level=ERROR/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"ERROR\"},"
                        + "\"size\":12345,\"modificationTime\":1732000000500,\"dataChange\":true,"
                        + "\"stats\":\"{\\\"numRecords\\\":10}\"}}",
                "{\"add\":{\"path\":\"level=INFO/part-0.snappy.parquet\",\"partitionValues\":{\"level\":\"INFO\"},"
                        + "\"size\":67890,\"modificationTime\":1732000000500,\"dataChange\":true}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    @Test
    void latest_matchesSnapshotManagerExactly(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);

        Snapshot direct = new SnapshotManager(tableRoot).latest();
        NormalizedSnapshot normalized = new DeltaTableFormat(tableRoot).latest();

        assertEquals(direct.getVersion(), normalized.getVersion());
        assertEquals(direct.getMetadata().getSchemaString(), normalized.getSchemaString());
        assertEquals(direct.getMetadata().getPartitionColumns(), normalized.getPartitionColumns());
        assertEquals(direct.getActiveFileCount(), normalized.getFiles().size());

        List<AddFile> directFiles = direct.getActiveFiles().stream()
                .sorted(Comparator.comparing(AddFile::getPath)).toList();
        List<DataFileRef> normalizedFiles = normalized.getFiles().stream()
                .sorted(Comparator.comparing(DataFileRef::getPath)).toList();

        for (int i = 0; i < directFiles.size(); i++) {
            AddFile expected = directFiles.get(i);
            DataFileRef actual = normalizedFiles.get(i);
            assertEquals(expected.getPath(), actual.getPath());
            assertEquals(expected.getPartitionValues(), actual.getPartitionValues());
            assertEquals(expected.getSize(), actual.getSizeBytes());
            assertEquals(expected.getStats(), actual.getStats());
        }
    }

    @Test
    void atVersion_delegatesCorrectly(@TempDir Path tableRoot) throws Exception {
        writeFixtureCommit(tableRoot);

        NormalizedSnapshot v0 = new DeltaTableFormat(tableRoot).atVersion(0);
        assertEquals(0, v0.getVersion());
        assertEquals(2, v0.getFiles().size());
    }
}
