package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.MetadataAction;
import com.labuk.labukdb.core.log.action.ProtocolAction;
import com.labuk.labukdb.core.log.action.SingleAction;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a single-part Delta checkpoint file — a Parquet file whose schema
 * mirrors the commit-log action shape (add/remove/metaData/protocol), one
 * top-level column per action kind, exactly one populated per row, the
 * unpopulated ones absent rather than a real Parquet {@code null} (Delta's
 * own checkpoint writer convention, mirrored here).
 *
 * <b>Read this before trusting anything in this class:</b> this hand-parses
 * a NESTED Parquet schema — structs for {@code add}/{@code remove}/
 * {@code metaData}/{@code protocol}, MAP-encoded {@code partitionValues}/
 * {@code configuration}, LIST-encoded {@code partitionColumns} — using
 * parquet-mr's raw {@code Group} API, field names assumed from the Delta
 * protocol spec and the standard Parquet MAP/LIST 3-level encoding
 * ({@code key_value}/{@code key}/{@code value} for maps, {@code list}/
 * {@code element} for lists — part of the Parquet format spec itself, not
 * writer-specific, so reasonably safe to assume). None of this has been
 * verified against a real checkpoint file — this environment has no way
 * to produce or fetch one. If checkpoint loading throws, the exception
 * message and stack trace pointing at a specific {@code getGroup}/
 * {@code getString} call is exactly what to check first against a real
 * checkpoint's schema (printable via {@code parquet-tools schema} or this
 * project's own {@code ParquetToArrowReader.readSchema} on the checkpoint
 * file directly).
 *
 * Only {@code add}, {@code metaData}, and {@code protocol} rows are parsed
 * into actions — {@code remove} rows, present in newer checkpoint versions
 * for VACUUM tombstone bookkeeping, are skipped, since {@link
 * SnapshotManager} only consumes add/metaData/protocol from a checkpoint
 * (see its javadoc for why: a checkpoint's add-rows already represent the
 * fully-resolved live set).
 */
final class CheckpointReader {

    List<SingleAction> read(LogStorage storage, String checkpointFileName) {
        // Both single-part and multi-part files use the same Parquet schema
        // and the same Group-navigation logic — the difference is only in
        // how many files SnapshotManager calls this method with, not in
        // anything about the Parquet format itself.
        List<SingleAction> actions = new ArrayList<>();
        try {
            InputFile inputFile = storage.openParquetInputFile(checkpointFileName);
            try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
                MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

                PageReadStore pages;
                while ((pages = fileReader.readNextRowGroup()) != null) {
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    long rowCount = pages.getRowCount();
                    for (long r = 0; r < rowCount; r++) {
                        Group row = recordReader.read();
                        SingleAction action = toSingleAction(row);
                        if (action != null) {
                            actions.add(action);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read checkpoint file " + checkpointFileName, e);
        }
        return actions;
    }

    private SingleAction toSingleAction(Group row) {
        if (has(row, "add")) {
            return SingleAction.ofAdd(readAdd(row.getGroup("add", 0)));
        }
        if (has(row, "metaData")) {
            return SingleAction.ofMetaData(readMetaData(row.getGroup("metaData", 0)));
        }
        if (has(row, "protocol")) {
            return SingleAction.ofProtocol(readProtocol(row.getGroup("protocol", 0)));
        }
        return null; // remove/txn/domainMetadata rows — intentionally skipped, see class javadoc
    }

    private AddFile readAdd(Group add) {
        String path = getString(add, "path");
        Map<String, String> partitionValues = readStringMap(add, "partitionValues");
        long size = getLong(add, "size");
        long modificationTime = getLong(add, "modificationTime");
        boolean dataChange = has(add, "dataChange") && add.getBoolean("dataChange", 0);
        String stats = has(add, "stats") ? add.getString("stats", 0) : null;
        return new AddFile(path, partitionValues, size, modificationTime, dataChange, stats);
    }

    private MetadataAction readMetaData(Group metaData) {
        String id = getString(metaData, "id");
        String schemaString = getString(metaData, "schemaString");
        List<String> partitionColumns = readStringList(metaData, "partitionColumns");
        Map<String, String> configuration = readStringMap(metaData, "configuration");
        Long createdTime = has(metaData, "createdTime") ? metaData.getLong("createdTime", 0) : null;
        MetadataAction action = new MetadataAction(id, schemaString, partitionColumns, configuration, createdTime);
        if (has(metaData, "format")) {
            Group format = metaData.getGroup("format", 0);
            String provider = getString(format, "provider");
            Map<String, String> options = readStringMap(format, "options");
            action.setFormat(new MetadataAction.Format(provider, options));
        }
        return action;
    }

    private ProtocolAction readProtocol(Group protocol) {
        int minReader = has(protocol, "minReaderVersion") ? protocol.getInteger("minReaderVersion", 0) : 0;
        int minWriter = has(protocol, "minWriterVersion") ? protocol.getInteger("minWriterVersion", 0) : 0;
        return new ProtocolAction(minReader, minWriter);
    }

    // --- Parquet MAP/LIST logical-type decoding, standard 3-level encoding ---

    private Map<String, String> readStringMap(Group parent, String fieldName) {
        Map<String, String> result = new LinkedHashMap<>();
        if (!has(parent, fieldName)) {
            return result;
        }
        Group mapGroup = parent.getGroup(fieldName, 0);
        int entryCount = mapGroup.getFieldRepetitionCount("key_value");
        for (int i = 0; i < entryCount; i++) {
            Group entry = mapGroup.getGroup("key_value", i);
            String key = entry.getString("key", 0);
            String value = has(entry, "value") ? entry.getString("value", 0) : null;
            result.put(key, value);
        }
        return result;
    }

    private List<String> readStringList(Group parent, String fieldName) {
        List<String> result = new ArrayList<>();
        if (!has(parent, fieldName)) {
            return result;
        }
        Group listGroup = parent.getGroup(fieldName, 0);
        int elementCount = listGroup.getFieldRepetitionCount("list");
        for (int i = 0; i < elementCount; i++) {
            Group listEntry = listGroup.getGroup("list", i);
            if (has(listEntry, "element")) {
                result.add(listEntry.getString("element", 0));
            }
        }
        return result;
    }

    // --- small helpers so every optional-field check reads the same way ---

    /**
     * {@code getFieldRepetitionCount} throws {@code InvalidRecordException}
     * if {@code fieldName} isn't part of this group's schema AT ALL (a
     * field this file's writer never declared — a genuinely older
     * checkpoint, or a hand-built test fixture missing it), as opposed to
     * being declared but simply absent from this one row (which it
     * reports as a repetition count of 0, safely). {@code containsField}
     * is checked first so both cases mean "not present" here, rather than
     * the schema-level case throwing — confirmed by a real regression this
     * exact distinction caused once {@link #readMetaData} started looking
     * for {@code format}, which an older/hand-built schema may never have
     * declared at all.
     */
    private static boolean has(Group group, String fieldName) {
        return group.getType().containsField(fieldName) && group.getFieldRepetitionCount(fieldName) > 0;
    }

    private static String getString(Group group, String fieldName) {
        return has(group, fieldName) ? group.getString(fieldName, 0) : null;
    }

    private static long getLong(Group group, String fieldName) {
        return has(group, fieldName) ? group.getLong(fieldName, 0) : 0L;
    }
}
