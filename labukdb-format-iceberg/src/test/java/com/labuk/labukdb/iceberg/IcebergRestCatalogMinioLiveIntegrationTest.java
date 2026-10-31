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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The first <b>full data write-then-read round trip</b> through a real
 * Iceberg REST catalog server backed by real S3-compatible storage —
 * closing the specific gap {@link IcebergRestCatalogLiveIntegrationTest}'s
 * own javadoc names as out of scope there ("metadata/wire-protocol
 * operations only, not a full data write-then-read round trip ... running
 * the reference REST server with an S3-backed warehouse ... hit a real
 * constraint"). That constraint turned out to be solvable, not
 * architectural: {@code tabulario/iceberg-rest} just needs {@code
 * CATALOG_WAREHOUSE=s3a://<bucket>/} (not {@code s3://}) so the locations
 * it returns resolve against this project's own {@code MinioConfiguration}
 * (which only registers {@code fs.s3a.impl}), plus the standard {@code
 * CATALOG_S3_*}/{@code AWS_*} env vars pointed at the same real MinIO
 * instance every other MinIO test in this project already uses.
 *
 * Uses the exact {@link Catalog}-based constructors {@link
 * IcebergTableWriter#IcebergTableWriter(Catalog, TableIdentifier)}/{@link
 * IcebergTableFormat#IcebergTableFormat(Catalog, TableIdentifier)} already
 * built for this (see their own javadoc) — both derive their {@code
 * TableLocation} from the loaded {@link org.apache.iceberg.Table}'s own
 * {@code location()} via {@code TableLocation.fromStringWithEnvironmentConfig},
 * so no new production code was needed, only this test.
 *
 * Opt-in by design: skips (not fails) when any of {@code ICEBERG_REST_URI}/
 * {@code MINIO_ENDPOINT}/{@code MINIO_ACCESS_KEY}/{@code MINIO_SECRET_KEY}
 * isn't set.
 */
class IcebergRestCatalogMinioLiveIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    private BufferAllocator allocator;
    private Catalog catalog;
    private Namespace namespace;
    private TableIdentifier id;

    @BeforeEach
    void checkPreconditionsAndConnect() {
        String restUri = System.getenv("ICEBERG_REST_URI");
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(restUri != null && !restUri.isBlank(), "ICEBERG_REST_URI not set — skipping live REST+MinIO round-trip test");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live REST+MinIO round-trip test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live REST+MinIO round-trip test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live REST+MinIO round-trip test");

        allocator = new RootAllocator(Long.MAX_VALUE);
        catalog = IcebergRestConfiguration.connect();

        String namespaceName = "labukdb_rest_minio_test_" + UUID.randomUUID().toString().replace("-", "");
        namespace = Namespace.of(namespaceName);
        ((SupportsNamespaces) catalog).createNamespace(namespace);
        id = TableIdentifier.of(namespace, "logs");
    }

    @AfterEach
    void cleanUp() {
        if (catalog != null && id != null && catalog.tableExists(id)) {
            catalog.dropTable(id, true); // purge=true: also deletes the real MinIO data files this test wrote
        }
        if (catalog != null && namespace != null) {
            ((SupportsNamespaces) catalog).dropNamespace(namespace);
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    private VectorSchemaRoot buildBatch() {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        batch.allocateNew();
        VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
        BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
        String[] services = {"auth-svc", "payments-svc"};
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, i + 1L);
        }
        batch.setRowCount(services.length);
        return batch;
    }

    @Test
    void createTableViaRest_writeAndReadBackThroughRealMinio_dataSurvives() throws Exception {
        catalog.createTable(id, ICEBERG_SCHEMA, SPEC);
        String tableLocation = catalog.loadTable(id).location();
        assertTrue(tableLocation.startsWith("s3a://"),
                "the REST server's reported table location must resolve to a real s3a:// path this JVM can use: " + tableLocation);

        try (VectorSchemaRoot batch = buildBatch()) {
            new IcebergTableWriter(catalog, id).appendBatch(batch, Map.of("level", "ERROR"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(catalog, id).latest();
        assertEquals(1, snapshot.getFiles().size(), "exactly one committed data file");
        assertEquals(List.of("level"), snapshot.getPartitionColumns());

        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals(Map.of("level", "ERROR"), file.getPartitionValues());
        assertTrue(file.getPath().startsWith("s3a://"), "Iceberg stores the full absolute URI for a remote file: " + file.getPath());

        TableLocation dataFileLocation = TableLocation.fromStringWithEnvironmentConfig(file.getPath());
        try (VectorSchemaRoot result = new ParquetToArrowReader().read(
                dataFileLocation, allocator, List.of("service", "amount", "level"), null, file.getPartitionValues())) {
            assertEquals(2, result.getRowCount());
            VarCharVector serviceVector = (VarCharVector) result.getVector("service");
            BigIntVector amountVector = (BigIntVector) result.getVector("amount");
            VarCharVector levelVector = (VarCharVector) result.getVector("level");
            assertEquals("auth-svc", new String(serviceVector.get(0), StandardCharsets.UTF_8));
            assertEquals("payments-svc", new String(serviceVector.get(1), StandardCharsets.UTF_8));
            assertEquals(1L, amountVector.get(0));
            assertEquals(2L, amountVector.get(1));
            assertEquals("ERROR", new String(levelVector.get(0), StandardCharsets.UTF_8));
            assertEquals("ERROR", new String(levelVector.get(1), StandardCharsets.UTF_8));
        }
    }
}
