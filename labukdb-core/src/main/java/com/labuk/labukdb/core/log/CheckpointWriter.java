package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.MetadataAction;
import com.labuk.labukdb.core.log.action.ProtocolAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Map;

/**
 * Writes a single-part Delta checkpoint — the counterpart {@link
 * CheckpointReader} never had. Materializes one {@link Snapshot} (a fully
 * resolved point-in-time file set, from {@link SnapshotManager}) into the
 * exact nested Parquet shape {@link CheckpointReader} already parses, plus
 * the {@code _last_checkpoint} pointer file that tells a reader where to
 * find it.
 *
 * <b>Schema scope, deliberate:</b> writes only the base fields real Delta's
 * own checkpoint schema has always had — {@code add}:
 * {@code path}/{@code partitionValues}/{@code size}/{@code
 * modificationTime}/{@code dataChange}/{@code stats}; {@code metaData}:
 * {@code id}/{@code schemaString}/{@code partitionColumns}/{@code
 * configuration}/{@code createdTime}; {@code protocol}: {@code
 * minReaderVersion}/{@code minWriterVersion} — confirmed, via a genuine
 * local Spark 3.5.3 + Delta 3.0.0 run inspected directly, to be a strict
 * subset of a real modern checkpoint's schema (which additionally carries
 * {@code tags}/{@code deletionVector}/{@code baseRowId}/{@code
 * defaultRowCommitVersion}/{@code partitionValues_parsed} on {@code add};
 * {@code name}/{@code description}/{@code format} on {@code metaData};
 * {@code readerFeatures}/{@code writerFeatures} on {@code protocol}). This
 * project doesn't implement deletion vectors, row tracking, or table
 * features, so those columns would be structurally present but always
 * empty — omitted rather than faked. A real Delta reader (including Spark)
 * tolerates a checkpoint narrower than its own newest schema the same way
 * it tolerates an old table's protocol version — this is exactly the shape
 * a genuinely old (pre-these-features) Delta checkpoint already has, not a
 * novel shape invented here. {@code txn}/{@code remove}/{@code
 * domainMetadata} rows are never written, mirroring {@link CheckpointReader}
 * never reading them (a checkpoint's add-rows already represent the
 * fully-resolved live set — remove/txn/domainMetadata exist for different
 * purposes {@link SnapshotManager} doesn't need).
 *
 * <b>Never multi-part.</b> A real Delta writer splits a checkpoint across
 * multiple Parquet files once the active file count crosses a size
 * threshold (Spark's default: 50 MB of resulting checkpoint data, roughly
 * tens of thousands of files) — {@link CheckpointReader} already supports
 * reading that shape, written by some other engine, but this project's own
 * writer always produces one file, regardless of table size. Splitting is a
 * scale optimization this project's own tables don't need yet; not
 * attempted here rather than attempted incorrectly.
 *
 * <b>{@code _last_checkpoint}</b> is written with only the two fields the
 * Delta protocol actually requires — {@code version} and {@code size} (row
 * count) — not Spark's own additional {@code sizeInBytes}/{@code
 * numOfAddFiles}/{@code checkpointSchema}/{@code checksum} (all optional,
 * confirmed from a real Spark-written one): those exist so a reader can
 * skip work it would otherwise redo, not because a correct reader requires
 * them.
 */
final class CheckpointWriter {

    private static final String CHECKPOINT_SCHEMA_STRING = """
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
                optional group format {
                  optional binary provider (UTF8);
                  optional group options (MAP) {
                    repeated group key_value {
                      required binary key (UTF8);
                      optional binary value (UTF8);
                    }
                  }
                }
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

    private static final MessageType CHECKPOINT_SCHEMA = MessageTypeParser.parseMessageType(CHECKPOINT_SCHEMA_STRING);

    /**
     * @return the number of rows written (add rows, plus one each for
     *         metaData/protocol if present) — exactly what {@code
     *         _last_checkpoint}'s own {@code size} field must report.
     */
    int write(LogStorage storage, Snapshot snapshot) throws IOException {
        String fileName = CommitFileNames.checkpointFileName(snapshot.getVersion());
        SimpleGroupFactory factory = new SimpleGroupFactory(CHECKPOINT_SCHEMA);

        Configuration conf = storage.hadoopConf();
        GroupWriteSupport.setSchema(CHECKPOINT_SCHEMA, conf);

        int rowCount = 0;
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(storage.hadoopPath(fileName))
                .withConf(conf).withType(CHECKPOINT_SCHEMA).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {

            for (AddFile add : snapshot.getActiveFiles()) {
                writer.write(buildAddRow(factory, add));
                rowCount++;
            }
            if (snapshot.getMetadata() != null) {
                writer.write(buildMetaDataRow(factory, snapshot.getMetadata()));
                rowCount++;
            }
            if (snapshot.getProtocol() != null) {
                writer.write(buildProtocolRow(factory, snapshot.getProtocol()));
                rowCount++;
            }
        }

        storage.writeTextFileOverwriting(CommitFileNames.LAST_CHECKPOINT,
                "{\"version\":" + snapshot.getVersion() + ",\"size\":" + rowCount + "}\n");
        return rowCount;
    }

    private Group buildAddRow(SimpleGroupFactory factory, AddFile add) {
        Group row = factory.newGroup();
        Group addGroup = row.addGroup("add");
        addGroup.append("path", add.getPath());
        appendStringMap(addGroup.addGroup("partitionValues"), add.getPartitionValues());
        addGroup.append("size", add.getSize());
        addGroup.append("modificationTime", add.getModificationTime());
        addGroup.append("dataChange", add.isDataChange());
        if (add.getStats() != null) {
            addGroup.append("stats", add.getStats());
        }
        return row;
    }

    private Group buildMetaDataRow(SimpleGroupFactory factory, MetadataAction meta) {
        Group row = factory.newGroup();
        Group metaGroup = row.addGroup("metaData");
        if (meta.getId() != null) {
            metaGroup.append("id", meta.getId());
        }
        Group formatGroup = metaGroup.addGroup("format");
        formatGroup.append("provider", meta.getFormat().getProvider());
        appendStringMap(formatGroup.addGroup("options"), meta.getFormat().getOptions());
        if (meta.getSchemaString() != null) {
            metaGroup.append("schemaString", meta.getSchemaString());
        }
        Group partitionColumns = metaGroup.addGroup("partitionColumns");
        for (String column : meta.getPartitionColumns()) {
            partitionColumns.addGroup("list").append("element", column);
        }
        appendStringMap(metaGroup.addGroup("configuration"), meta.getConfiguration());
        if (meta.getCreatedTime() != null) {
            metaGroup.append("createdTime", meta.getCreatedTime());
        }
        return row;
    }

    private Group buildProtocolRow(SimpleGroupFactory factory, ProtocolAction protocol) {
        Group row = factory.newGroup();
        Group protocolGroup = row.addGroup("protocol");
        protocolGroup.append("minReaderVersion", protocol.getMinReaderVersion());
        protocolGroup.append("minWriterVersion", protocol.getMinWriterVersion());
        return row;
    }

    private void appendStringMap(Group mapGroup, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Group keyValue = mapGroup.addGroup("key_value");
            keyValue.append("key", entry.getKey());
            if (entry.getValue() != null) {
                keyValue.append("value", entry.getValue());
            }
        }
    }
}
