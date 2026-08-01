package com.labuk.deltaflight.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A small, real catalog — not Unity Catalog, doesn't try to be. Just what's
 * actually needed: a name resolves to a table root, persisted to a JSON
 * file so it survives across CLI invocations and server restarts.
 *
 * <b>Why this matters beyond convenience:</b> handing an AI agent or MCP
 * tool a raw filesystem path (right drive letter, right slash direction,
 * exact casing) is exactly the kind of thing that goes wrong in practice —
 * and arguably shouldn't be something an agent needs to know about your
 * disk layout at all. A catalog turns "the logs table" into a name an LLM
 * can just use.
 *
 * Thread-safe (backed by a {@code ConcurrentHashMap}), and every mutation
 * writes the whole file back out immediately — this is a small,
 * infrequently-changed registry, not a high-throughput store, so simple
 * beats clever here.
 */
public final class TableCatalog {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path catalogFile;
    private final Map<String, String> entries = new ConcurrentHashMap<>();

    public TableCatalog(Path catalogFile) {
        this.catalogFile = catalogFile;
        load();
    }

    /** Default location: {@code ~/.delta-flight-engine/catalog.json}. */
    public static TableCatalog atDefaultLocation() {
        Path defaultFile = Path.of(System.getProperty("user.home"), ".delta-flight-engine", "catalog.json");
        return new TableCatalog(defaultFile);
    }

    public synchronized void register(String name, Path tableRoot) {
        entries.put(name, tableRoot.toAbsolutePath().normalize().toString());
        save();
    }

    public synchronized boolean unregister(String name) {
        boolean existed = entries.remove(name) != null;
        if (existed) {
            save();
        }
        return existed;
    }

    public Optional<Path> resolve(String name) {
        String path = entries.get(name);
        return path == null ? Optional.empty() : Optional.of(Path.of(path));
    }

    /** Sorted by name — stable, predictable output for a CLI listing or ListFlights. */
    public Map<String, Path> listAll() {
        Map<String, Path> result = new TreeMap<>();
        entries.forEach((name, path) -> result.put(name, Path.of(path)));
        return result;
    }

    private void load() {
        if (!Files.exists(catalogFile)) {
            return;
        }
        try {
            Map<String, String> loaded = MAPPER.readValue(catalogFile.toFile(),
                    MAPPER.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, String.class));
            entries.putAll(loaded);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read catalog file " + catalogFile, e);
        }
    }

    private void save() {
        try {
            Files.createDirectories(catalogFile.getParent());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(catalogFile.toFile(), new TreeMap<>(entries));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write catalog file " + catalogFile, e);
        }
    }
}
