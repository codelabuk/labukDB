package com.labuk.labukdb.core.catalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TableCatalogTest {

    @Test
    void registerThenResolve_normalizesLocalPathToAbsolute(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", "some-relative-table");

        Optional<String> resolved = catalog.resolve("logs");
        assertTrue(resolved.isPresent());
        assertTrue(Path.of(resolved.get()).isAbsolute());
    }

    @Test
    void registerThenResolve_remoteUri_isLeftUntouched(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        String uri = "s3a://some-bucket/some-table";
        catalog.register("logs", uri);

        assertEquals(uri, catalog.resolve("logs").orElseThrow(),
                "a remote URI must be stored exactly as given, not mangled through local-path normalization");
    }

    @Test
    void isRemoteUri_detectsSchemesCorrectly() {
        assertTrue(TableCatalog.isRemoteUri("s3a://bucket/table"));
        assertTrue(TableCatalog.isRemoteUri("abfss://container@account.dfs.core.windows.net/table"));
        assertFalse(TableCatalog.isRemoteUri("D:\\repo\\lakehouse\\logs_delta"));
        assertFalse(TableCatalog.isRemoteUri("/home/user/lakehouse/logs"));
        assertFalse(TableCatalog.isRemoteUri("some-relative-table"));
    }

    @Test
    void resolvingUnknownName_returnsEmpty(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        assertTrue(catalog.resolve("nonexistent").isEmpty());
    }

    @Test
    void listAll_returnsEverythingRegistered(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", "table-a");
        catalog.register("metrics", "table-b");

        assertEquals(2, catalog.listAll().size());
        assertTrue(catalog.listAll().containsKey("logs"));
        assertTrue(catalog.listAll().containsKey("metrics"));
    }

    @Test
    void unregister_removesTheEntry(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", "table-a");

        assertTrue(catalog.unregister("logs"));
        assertTrue(catalog.resolve("logs").isEmpty());
    }

    @Test
    void unregister_unknownName_returnsFalse(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        assertFalse(catalog.unregister("nonexistent"));
    }

    @Test
    void persistsAcrossInstances_viaTheSameFile(@TempDir Path tempDir) {
        Path catalogFile = tempDir.resolve("catalog.json");

        TableCatalog first = new TableCatalog(catalogFile);
        first.register("logs", "table-a");

        TableCatalog second = new TableCatalog(catalogFile);
        assertTrue(second.resolve("logs").isPresent(),
                "a fresh TableCatalog instance pointed at the same file should see prior registrations");
    }
}
