package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.MetadataAction;
import com.labuk.labukdb.core.log.action.ProtocolAction;
import com.labuk.labukdb.core.log.action.SingleAction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trips a hand-built {@link Snapshot} through {@link CheckpointWriter}
 * then {@link CheckpointReader} — the mechanical proof that what this
 * project's own writer produces is exactly what its own reader expects
 * (independent of {@link CheckpointerIntegrationTest}'s end-to-end proof via
 * real commits, and of {@link CheckpointReaderRealSparkFixtureTest}'s proof
 * against a genuine Spark-produced file).
 */
class CheckpointWriterTest {

    @Test
    void writeThenRead_roundTripsAddMetaDataAndProtocolCorrectly(@TempDir Path dir) throws Exception {
        Map<String, AddFile> activeFiles = new LinkedHashMap<>();
        activeFiles.put("level=ERROR/part-0.parquet",
                new AddFile("level=ERROR/part-0.parquet", Map.of("level", "ERROR"), 1234L, 1732000000000L, true,
                        "{\"numRecords\":10}"));
        activeFiles.put("level=WARN/part-0.parquet",
                new AddFile("level=WARN/part-0.parquet", Map.of("level", "WARN"), 500L, 1732000000001L, true, null));

        MetadataAction metadata = new MetadataAction("table-id-123", "{\"type\":\"struct\"}",
                List.of("level"), Map.of("delta.appendOnly", "true"), 1700000000000L);
        ProtocolAction protocol = new ProtocolAction(1, 2);

        Snapshot snapshot = new Snapshot(5, activeFiles, metadata, protocol);

        LogStorage storage = new LocalNioLogStorage(dir);
        int rowCount = new CheckpointWriter().write(storage, snapshot);
        assertEquals(4, rowCount, "2 add rows + 1 metaData + 1 protocol");

        String checkpointFileName = CommitFileNames.checkpointFileName(5);
        assertTrue(Files.exists(dir.resolve(checkpointFileName)));

        List<SingleAction> actions = new CheckpointReader().read(storage, checkpointFileName);
        assertEquals(4, actions.size());

        List<AddFile> adds = actions.stream().map(SingleAction::getAdd).filter(a -> a != null).toList();
        assertEquals(2, adds.size());
        AddFile errorFile = adds.stream().filter(a -> "level=ERROR/part-0.parquet".equals(a.getPath())).findFirst().orElseThrow();
        assertEquals("ERROR", errorFile.getPartitionValues().get("level"));
        assertEquals(1234L, errorFile.getSize());
        assertTrue(errorFile.isDataChange());
        assertTrue(errorFile.getStats().contains("numRecords"));
        AddFile warnFile = adds.stream().filter(a -> "level=WARN/part-0.parquet".equals(a.getPath())).findFirst().orElseThrow();
        assertNull(warnFile.getStats(), "a null stats field must round-trip as absent, not an empty string");

        MetadataAction readMeta = actions.stream().map(SingleAction::getMetaData).filter(m -> m != null).findFirst().orElseThrow();
        assertEquals("table-id-123", readMeta.getId());
        assertEquals(List.of("level"), readMeta.getPartitionColumns());
        assertEquals("true", readMeta.getConfiguration().get("delta.appendOnly"));
        assertEquals(1700000000000L, readMeta.getCreatedTime());

        ProtocolAction readProtocol = actions.stream().map(SingleAction::getProtocol).filter(p -> p != null).findFirst().orElseThrow();
        assertEquals(1, readProtocol.getMinReaderVersion());
        assertEquals(2, readProtocol.getMinWriterVersion());
    }

    @Test
    void write_producesAMinimalValidLastCheckpointFile(@TempDir Path dir) throws Exception {
        Map<String, AddFile> activeFiles = new LinkedHashMap<>();
        activeFiles.put("a.parquet", new AddFile("a.parquet", Map.of(), 10L, 1L, true, null));
        Snapshot snapshot = new Snapshot(3, activeFiles, new MetadataAction("id", "{}", List.of(), Map.of(), 1L),
                new ProtocolAction(1, 2));

        new CheckpointWriter().write(new LocalNioLogStorage(dir), snapshot);

        String content = Files.readString(dir.resolve("_last_checkpoint"));
        assertTrue(content.contains("\"version\":3"), content);
        assertTrue(content.contains("\"size\":3"), content); // 1 add + 1 metaData + 1 protocol
    }

    @Test
    void write_overwritesAPreviousLastCheckpoint_notAppendsOrFailsOnExists(@TempDir Path dir) throws Exception {
        LogStorage storage = new LocalNioLogStorage(dir);
        Map<String, AddFile> files1 = new LinkedHashMap<>();
        files1.put("a.parquet", new AddFile("a.parquet", Map.of(), 10L, 1L, true, null));
        new CheckpointWriter().write(storage, new Snapshot(1, files1, new MetadataAction("id", "{}", List.of(), Map.of(), 1L), new ProtocolAction(1, 2)));

        Map<String, AddFile> files2 = new LinkedHashMap<>();
        files2.put("a.parquet", new AddFile("a.parquet", Map.of(), 10L, 1L, true, null));
        files2.put("b.parquet", new AddFile("b.parquet", Map.of(), 20L, 2L, true, null));
        new CheckpointWriter().write(storage, new Snapshot(2, files2, new MetadataAction("id", "{}", List.of(), Map.of(), 1L), new ProtocolAction(1, 2)));

        String content = Files.readString(dir.resolve("_last_checkpoint"));
        assertTrue(content.contains("\"version\":2"), "must point at the newer checkpoint, not the first one: " + content);
    }
}
