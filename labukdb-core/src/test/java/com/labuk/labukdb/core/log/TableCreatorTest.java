package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the capability this project never had before: creating a
 * brand-new Delta table from nothing, rather than every test/fixture
 * hand-writing an initial commit directly.
 */
class TableCreatorTest {

    private Schema buildSchema() {
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        return new Schema(List.of(service, amount));
    }

    @Test
    void createTable_localDisk_producesAReadableEmptyTable(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        long version = new TableCreator().createTable(location, buildSchema(), List.of("level"), null);
        assertEquals(0, version);

        Snapshot snapshot = new SnapshotManager(tableRoot).latest();
        assertEquals(0, snapshot.getVersion());
        assertEquals(0, snapshot.getActiveFileCount());
        assertEquals(List.of("level"), snapshot.getMetadata().getPartitionColumns());
        assertTrue(snapshot.getMetadata().getSchemaString().contains("\"service\""));
        assertTrue(snapshot.getMetadata().getSchemaString().contains("\"amount\""));
    }

    @Test
    void createTable_tableAlreadyExists_throwsClearError(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, buildSchema(), List.of(), null);

        IOException thrown = assertThrows(IOException.class,
                () -> new TableCreator().createTable(location, buildSchema(), List.of(), null));
        assertTrue(thrown.getMessage().contains("already exists"),
                "error must explain why, not surface a raw NIO/Hadoop exception: " + thrown.getMessage());
    }

    @Test
    void createTable_thenAppendBatch_realReadBackRoundTrip(@TempDir Path tableRoot) throws Exception {
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, buildSchema(), List.of(), Map.of("owner", "table-creator-test"));

        Snapshot snapshot = new SnapshotManager(tableRoot).latest();
        assertEquals(Map.of("owner", "table-creator-test"), snapshot.getMetadata().getConfiguration());

        // A freshly created table must be immediately usable by the
        // existing append path -- proves TableCreator's commit is a real,
        // valid Delta commit, not just something SnapshotManager happens
        // to tolerate.
        try (org.apache.arrow.memory.BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE)) {
            Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
            try (org.apache.arrow.vector.VectorSchemaRoot batch =
                         org.apache.arrow.vector.VectorSchemaRoot.create(new Schema(List.of(service, amount)), allocator)) {
                batch.allocateNew();
                ((org.apache.arrow.vector.VarCharVector) batch.getVector("service"))
                        .setSafe(0, "auth-svc".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ((org.apache.arrow.vector.BigIntVector) batch.getVector("amount")).setSafe(0, 42L);
                batch.setRowCount(1);
                new CommitWriter(tableRoot).appendBatch(batch, Map.of());
            }
        }

        Snapshot after = new SnapshotManager(tableRoot).latest();
        assertEquals(1, after.getVersion());
        assertEquals(1, after.getActiveFileCount());
    }
}
