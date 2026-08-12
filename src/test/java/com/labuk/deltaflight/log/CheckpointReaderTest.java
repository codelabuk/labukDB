package com.labuk.deltaflight.log;

import com.labuk.deltaflight.log.action.AddFile;
import com.labuk.deltaflight.log.action.MetadataAction;
import com.labuk.deltaflight.log.action.ProtocolAction;
import com.labuk.deltaflight.log.action.SingleAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IMPORTANT SCOPE NOTE: this proves CheckpointReader's Group-navigation
 * logic (nested struct/map/list decoding) is mechanically correct against
 * a checkpoint file built with the schema this project ASSUMES matches
 * real Delta checkpoints. It does NOT prove that assumption is correct —
 * there's no real Spark-produced checkpoint file available to test
 * against here. If real checkpoint loading fails, this test still passing
 * means the bug is in the assumed schema, not in the parsing mechanics.
 */
class CheckpointReaderTest {

    private static final String CHECKPOINT_SCHEMA = """
            message checkpoint {
              optional group add {
                required binary path (UTF8);
                optional group partitionValues (MAP) {
                  repeated group key_value {
                    required binary key (UTF8);
                    optional binary value (UTF8);
                  }
                }
                required int64 size;
                required int64 modificationTime;
                required boolean dataChange;
                optional binary stats (UTF8);
              }
              optional group metaData {
                optional binary id (UTF8);
                optional binary schemaString (UTF8);
                optional group partitionColumns (LIST) {
                  repeated group list {
                    optional binary element (UTF8);
                  }
                }
                optional group configuration (MAP) {
                  repeated group key_value {
                    required binary key (UTF8);
                    optional binary value (UTF8);
                  }
                }
                optional int64 createdTime;
              }
              optional group protocol {
                optional int32 minReaderVersion;
                optional int32 minWriterVersion;
              }
            }""";

    private Path writeTestCheckpoint(Path dir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(CHECKPOINT_SCHEMA);
        Path file = dir.resolve("00000000000000000005.checkpoint.parquet");

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toString());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {

            SimpleGroupFactory factory = new SimpleGroupFactory(schema);

            Group addRow = factory.newGroup();
            Group add = addRow.addGroup("add");
            add.append("path", "level=ERROR/part-0.parquet");
            Group partitionValues = add.addGroup("partitionValues");
            Group pvEntry = partitionValues.addGroup("key_value");
            pvEntry.append("key", "level");
            pvEntry.append("value", "ERROR");
            add.append("size", 1234L);
            add.append("modificationTime", 1732000000000L);
            add.append("dataChange", true);
            add.append("stats", "{\"numRecords\":10}");
            writer.write(addRow);

            Group metaRow = factory.newGroup();
            Group metaData = metaRow.addGroup("metaData");
            metaData.append("id", "table-id-123");
            metaData.append("schemaString", "{\"type\":\"struct\"}");
            Group partitionColumns = metaData.addGroup("partitionColumns");
            Group pcEntry = partitionColumns.addGroup("list");
            pcEntry.append("element", "level");
            Group configuration = metaData.addGroup("configuration");
            Group cfgEntry = configuration.addGroup("key_value");
            cfgEntry.append("key", "delta.appendOnly");
            cfgEntry.append("value", "true");
            metaData.append("createdTime", 1700000000000L);
            writer.write(metaRow);

            Group protoRow = factory.newGroup();
            Group protocol = protoRow.addGroup("protocol");
            protocol.append("minReaderVersion", 1);
            protocol.append("minWriterVersion", 2);
            writer.write(protoRow);
        }
        return file;
    }

    @Test
    void readsAllThreeActionKinds_withCorrectNestedFields(@TempDir Path tempDir) throws Exception {
        writeTestCheckpoint(tempDir);
        LogStorage storage = new LocalNioLogStorage(tempDir);

        List<SingleAction> actions = new CheckpointReader().read(storage, "00000000000000000005.checkpoint.parquet");

        assertEquals(3, actions.size());

        AddFile add = actions.stream().map(SingleAction::getAdd).filter(a -> a != null).findFirst().orElseThrow();
        assertEquals("level=ERROR/part-0.parquet", add.getPath());
        assertEquals("ERROR", add.getPartitionValues().get("level"));
        assertEquals(1234L, add.getSize());
        assertTrue(add.isDataChange());
        assertTrue(add.getStats().contains("numRecords"));

        MetadataAction meta = actions.stream().map(SingleAction::getMetaData).filter(m -> m != null).findFirst().orElseThrow();
        assertEquals("table-id-123", meta.getId());
        assertEquals(List.of("level"), meta.getPartitionColumns());
        assertEquals("true", meta.getConfiguration().get("delta.appendOnly"));
        assertEquals(1700000000000L, meta.getCreatedTime());

        ProtocolAction protocol = actions.stream().map(SingleAction::getProtocol).filter(p -> p != null).findFirst().orElseThrow();
        assertEquals(1, protocol.getMinReaderVersion());
        assertEquals(2, protocol.getMinWriterVersion());
    }

    @Test
    void multiPartCheckpointFile_parsesWithSameLogicAsSinglePart(@TempDir Path tempDir) throws Exception {
        // Multi-part checkpoint files use the identical Parquet schema as
        // single-part ones — the difference is only in how many files
        // SnapshotManager calls read() with. Write a single file with a
        // multi-part filename and verify it parses correctly.
        MessageType schema = MessageTypeParser.parseMessageType(CHECKPOINT_SCHEMA);
        String multiPartName = "00000000000000000005.checkpoint.0000000001.0000000002.parquet";
        Path file = tempDir.resolve(multiPartName);

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toString());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group addRow = factory.newGroup();
            Group add = addRow.addGroup("add");
            add.append("path", "level=WARN/part-0.parquet");
            Group pv = add.addGroup("partitionValues");
            Group pvEntry = pv.addGroup("key_value");
            pvEntry.append("key", "level");
            pvEntry.append("value", "WARN");
            add.append("size", 100L);
            add.append("modificationTime", 1732000000000L);
            add.append("dataChange", false);
            writer.write(addRow);
        }

        LogStorage storage = new LocalNioLogStorage(tempDir);
        List<SingleAction> actions = new CheckpointReader().read(storage, multiPartName);

        assertEquals(1, actions.size());
        assertNotNull(actions.get(0).getAdd());
        assertEquals("level=WARN/part-0.parquet", actions.get(0).getAdd().getPath());
    }
}
