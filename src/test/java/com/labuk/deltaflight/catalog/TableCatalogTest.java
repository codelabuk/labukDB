package com.labuk.deltaflight.catalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TableCatalogTest {

    @Test
    void registerThenResolve_returnsAbsoluteNormalizedPath(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", Path.of("some-relative-table"));

        Optional<Path> resolved = catalog.resolve("logs");
        assertTrue(resolved.isPresent());
        assertTrue(resolved.get().isAbsolute());
    }

    @Test
    void resolvingUnknownName_returnsEmpty(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        assertTrue(catalog.resolve("nonexistent").isEmpty());
    }

    @Test
    void listAll_returnsEverythingRegistered(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", Path.of("table-a"));
        catalog.register("metrics", Path.of("table-b"));

        assertEquals(2, catalog.listAll().size());
        assertTrue(catalog.listAll().containsKey("logs"));
        assertTrue(catalog.listAll().containsKey("metrics"));
    }

    @Test
    void unregister_removesTheEntry(@TempDir Path tempDir) {
        TableCatalog catalog = new TableCatalog(tempDir.resolve("catalog.json"));
        catalog.register("logs", Path.of("table-a"));

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
        first.register("logs", Path.of("table-a"));

        TableCatalog second = new TableCatalog(catalogFile);
        assertTrue(second.resolve("logs").isPresent(),
                "a fresh TableCatalog instance pointed at the same file should see prior registrations");
    }
}
