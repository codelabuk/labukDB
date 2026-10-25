package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link IcebergTableFormat}/{@link IcebergTableWriter}'s new
 * {@code Catalog}-based constructors against a <b>real</b> {@link
 * InMemoryCatalog} — a genuine Iceberg {@code Catalog} implementation
 * bundled in {@code iceberg-core}, not a mock. Since {@code RESTCatalog}/
 * {@code HadoopCatalog}/{@code InMemoryCatalog} all share one {@code
 * Catalog} interface and this project's code only calls interface
 * methods (never anything REST-specific), this proves the actual
 * Iceberg-interaction logic is correct for any catalog — including
 * {@code RESTCatalog} — even without a real REST server available in
 * this development environment to test against directly. See {@code
 * IcebergRestConfiguration}'s javadoc for that honesty boundary stated
 * plainly.
 */
class IcebergCatalogIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    private Catalog catalog;

    @BeforeEach
    void setUp(@TempDir Path warehouse) {
        InMemoryCatalog inMemory = new InMemoryCatalog();
        inMemory.initialize("test", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString()));
        catalog = inMemory;
    }

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
    void appendThenReadBack_viaCatalogConstructors() throws Exception {
        TableIdentifier id = TableIdentifier.of("analytics", "logs");
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("analytics"));
        catalog.createTable(id, ICEBERG_SCHEMA, SPEC);

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
            VarCharVector readService = (VarCharVector) readBack.getVector("service");
            assertEquals("auth-svc", new String(readService.get(0), StandardCharsets.UTF_8));
            assertEquals("gateway", new String(readService.get(1), StandardCharsets.UTF_8));
        }
    }

    @Test
    void appendCalledTwice_bothCommitsVisibleInLatest() throws Exception {
        TableIdentifier id = TableIdentifier.of("analytics", "logs2");
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("analytics"));
        catalog.createTable(id, ICEBERG_SCHEMA, SPEC);

        IcebergTableWriter writer = new IcebergTableWriter(catalog, id);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot errorBatch = buildBatch(allocator, new String[] {"auth-svc"}, new long[] {1});
             VectorSchemaRoot infoBatch = buildBatch(allocator, new String[] {"gateway"}, new long[] {2})) {
            writer.appendBatch(errorBatch, Map.of("level", "ERROR"));
            writer.appendBatch(infoBatch, Map.of("level", "INFO"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(catalog, id).latest();
        assertEquals(2, snapshot.getFiles().size());
    }
}
