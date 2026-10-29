package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link IcebergTableCreator} actually produces a table {@link
 * IcebergTableWriter}/{@link IcebergTableFormat}'s existing, unmodified
 * constructors can immediately append to and read back — the same
 * create → append → read full-lifecycle proof {@code TableCreatorTest}
 * (Delta's equivalent) already gives, not just "doesn't throw."
 */
class IcebergTableCreatorTest {

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String[] services, long[] amounts) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        batch.allocateNew();
        VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
        BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, amounts[i]);
        }
        batch.setRowCount(services.length);
        return batch;
    }

    @Test
    void createTable_pathBased_thenAppendAndReadBack(@TempDir Path tableRoot) throws Exception {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field levelField = new Field("level", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema arrowSchema = new Schema(List.of(serviceField, amountField, levelField));

        new IcebergTableCreator().createTable(
                TableLocation.forLocal(tableRoot), arrowSchema, List.of("level"), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "gateway"}, new long[] {1, 2})) {
            new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(batch, Map.of("level", "ERROR"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        assertEquals(1, snapshot.getFiles().size());
        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));
        assertTrue(file.getPath().contains("level=ERROR"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                     new java.io.File(file.getPath()), allocator, List.of("service", "amount"), null, Map.of())) {
            assertEquals(2, readBack.getRowCount());
            VarCharVector readService = (VarCharVector) readBack.getVector("service");
            assertEquals("auth-svc", new String(readService.get(0), StandardCharsets.UTF_8));
        }
    }

    @Test
    void createTable_pathBased_unpartitioned_succeeds(@TempDir Path tableRoot) throws Exception {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema arrowSchema = new Schema(List.of(idField));

        new IcebergTableCreator().createTable(TableLocation.forLocal(tableRoot), arrowSchema, List.of(), Map.of());

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        assertEquals(0, snapshot.getFiles().size(), "a freshly created table has no data files yet");
    }

    @Test
    void createTable_pathBased_tableAlreadyExists_throws(@TempDir Path tableRoot) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema arrowSchema = new Schema(List.of(idField));

        new IcebergTableCreator().createTable(TableLocation.forLocal(tableRoot), arrowSchema, List.of(), Map.of());

        assertThrows(AlreadyExistsException.class, () ->
                new IcebergTableCreator().createTable(TableLocation.forLocal(tableRoot), arrowSchema, List.of(), Map.of()));
    }

    @Test
    void createTable_catalogBased_thenAppendAndReadBack(@TempDir Path warehouse) throws Exception {
        InMemoryCatalog inMemory = new InMemoryCatalog();
        inMemory.initialize("test", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString()));
        Catalog catalog = inMemory;
        TableIdentifier id = TableIdentifier.of("analytics", "logs");
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("analytics"));

        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field levelField = new Field("level", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema arrowSchema = new Schema(List.of(serviceField, amountField, levelField));

        new IcebergTableCreator().createTable(catalog, id, arrowSchema, List.of("level"), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "gateway"}, new long[] {1, 2})) {
            new IcebergTableWriter(catalog, id).appendBatch(batch, Map.of("level", "ERROR"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(catalog, id).latest();
        assertEquals(1, snapshot.getFiles().size());
        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                     new java.io.File(file.getPath()), allocator, List.of("service", "amount"), null, Map.of())) {
            assertEquals(2, readBack.getRowCount());
        }
    }
}
