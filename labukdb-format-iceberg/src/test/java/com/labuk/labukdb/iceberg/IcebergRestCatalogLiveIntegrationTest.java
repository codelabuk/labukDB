package com.labuk.labukdb.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The first real test of this project's {@link IcebergRestConfiguration}
 * against an actual REST catalog server over the wire — closing the
 * specific gap its own javadoc names: "the actual network round-trip
 * against a real REST server is not [tested]." Everything up to now was
 * proven against {@link org.apache.iceberg.inmemory.InMemoryCatalog}
 * in-process (real {@code Catalog} interface, but no HTTP involved at all).
 *
 * Run against {@code tabulario/iceberg-rest} (the standard open-source
 * Iceberg REST catalog reference implementation), e.g.:
 * {@code docker run -p 18181:8181 -e CATALOG_WAREHOUSE=/tmp/warehouse tabulario/iceberg-rest}.
 *
 * <b>Scope, stated plainly: metadata/wire-protocol operations only, not a
 * full write-then-read data round trip.</b> {@code IcebergTableWriter}'s
 * own data files are written directly by this project's JVM (via {@code
 * ArrowToParquetWriter}/{@code TableLocation}), not through the REST
 * server — so a full round trip needs the table's {@code location()} to
 * resolve to a path this JVM can actually write to, which conflicts with
 * running the reference REST server in a Linux container with its own
 * local or S3-backed warehouse in this (Windows-host) development
 * environment, without materializing cloud credentials into the
 * container (a real constraint hit while writing this, not glossed over).
 * What's proven here instead — genuinely new, not previously exercised
 * anywhere in this project — is that {@link IcebergRestConfiguration
 * #connect()} really speaks the Iceberg REST protocol correctly end to
 * end: connect, create a namespace, create a table with a real schema and
 * partition spec, load it back, and confirm the schema/spec survived a
 * real HTTP round trip to a real server, not an in-process call.
 *
 * Opt-in by design: skips (not fails) when {@code ICEBERG_REST_URI} isn't set.
 */
class IcebergRestCatalogLiveIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    @Test
    void connect_createNamespaceAndTable_loadBackWithCorrectSchemaAndSpec() {
        String uri = System.getenv("ICEBERG_REST_URI");
        assumeTrue(uri != null && !uri.isBlank(), "ICEBERG_REST_URI not set — skipping live REST catalog test");

        Catalog catalog = IcebergRestConfiguration.connect();

        String namespaceName = "labukdb_rest_test_" + UUID.randomUUID().toString().replace("-", "");
        Namespace namespace = Namespace.of(namespaceName);
        ((SupportsNamespaces) catalog).createNamespace(namespace);
        assertTrue(((SupportsNamespaces) catalog).namespaceExists(namespace),
                "namespace must actually exist on the real server after creation, not just appear to locally");

        TableIdentifier id = TableIdentifier.of(namespace, "logs");
        Table created = catalog.createTable(id, ICEBERG_SCHEMA, SPEC);
        assertNotNull(created);

        assertTrue(catalog.tableExists(id), "the real server must report the table as existing");

        Table loaded = catalog.loadTable(id);
        assertEquals(ICEBERG_SCHEMA.asStruct(), loaded.schema().asStruct(),
                "schema must round-trip exactly through the real REST server");
        assertEquals(List.of("level"), loaded.spec().fields().stream().map(f -> f.name()).toList(),
                "partition spec must round-trip exactly through the real REST server");

        catalog.dropTable(id);
        assertFalse(catalog.tableExists(id), "the real server must report the table as gone after drop");
    }
}
