package com.labuk.labukdb.core.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.encode.ArrowToDeltaSchema;
import com.labuk.labukdb.core.log.action.MetadataAction;
import com.labuk.labukdb.core.log.action.ProtocolAction;
import com.labuk.labukdb.core.log.action.SingleAction;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Creates a brand-new Delta table — the capability this project has
 * never had: every test and fixture up to this point hand-wrote an
 * initial commit directly. Writes version 0 only (a {@code protocol}
 * action plus a {@code metaData} action, no {@code add} actions) via the
 * same atomic create-if-absent primitive {@link CommitWriter} already
 * relies on, so "a table already exists at this location" is enforced
 * for free, not a separate check.
 *
 * <b>Scope, stated plainly:</b> local disk and remote (S3/MinIO/ADLS, via
 * {@link TableLocation}) both supported, same as {@link CommitWriter}.
 * Not exposed over Flight (no {@code DoAction} verb) or the CLI yet —
 * deliberately deferred; both need their own protocol/UX decisions this
 * class doesn't make for you.
 */
public final class TableCreator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * @param location         where the new table lives, local or remote
     * @param schema           the table's physical columns (not including
     *                         partition columns — matches {@link CommitWriter#appendBatch}'s
     *                         own convention of keeping partition values separate)
     * @param partitionColumns in the table's actual partition order
     * @param configuration    arbitrary table properties; {@code null} treated as empty
     * @return the new table's version — always {@code 0}
     * @throws IOException if a table already exists at {@code location}
     */
    public long createTable(TableLocation location, Schema schema, List<String> partitionColumns,
                             Map<String, String> configuration) throws IOException {
        if (!location.isRemote()) {
            // First write to a brand-new location -- unlike every other
            // write method here, there's no existing table whose
            // _delta_log directory is guaranteed to already exist.
            Files.createDirectories(location.localPath().resolve("_delta_log"));
        }

        String schemaString = ArrowToDeltaSchema.toSchemaString(schema);
        MetadataAction metaData = new MetadataAction(
                UUID.randomUUID().toString(),
                schemaString,
                partitionColumns,
                configuration == null ? Map.of() : configuration,
                System.currentTimeMillis());
        ProtocolAction protocol = new ProtocolAction(1, 2);

        StringBuilder commit = new StringBuilder();
        commit.append(MAPPER.writeValueAsString(SingleAction.ofProtocol(protocol))).append('\n');
        commit.append(MAPPER.writeValueAsString(SingleAction.ofMetaData(metaData))).append('\n');

        LogStorage storage = LogStorage.forLocation(location);
        try {
            storage.createCommitFile(CommitFileNames.jsonFileName(0), commit.toString());
        } catch (java.nio.file.FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException e) {
            // Two unrelated exception classes with the same simple name --
            // local (NIO) and remote (Hadoop) each throw their own, not a
            // shared supertype narrower than IOException. See
            // HadoopLogStorage's javadoc for why these aren't interchangeable.
            throw new IOException("A table already exists at " + location
                    + " -- createTable only creates brand-new tables, refusing to overwrite", e);
        }
        return 0L;
    }
}
