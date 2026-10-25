package com.labuk.labukdb.core.catalog;

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
import java.util.regex.Pattern;

/**
 * A small, real catalog: a name resolves to a table location, persisted to
 * a JSON file so it survives across CLI invocations and server restarts.
 *
 * <b>String-based, not Path-based — a deliberate fix, not the original
 * design.</b> The first version of this class took {@code java.nio.file.Path}
 * directly, which silently mangles any remote URI (a real bug: forcing
 * {@code s3a://bucket/table} through {@code Path.of(...).toAbsolutePath()}
 * does not produce anything usable). Storing the raw location string and
 * only normalizing local-looking paths — never touching anything that
 * looks like {@code scheme://...} — is what actually makes a catalog entry
 * capable of pointing at remote storage.
 *
 * <b>Still an interim limitation, stated plainly:</b> {@link
 * com.labuk.labukdb.cli.TableResolver} and {@code LabukDbFlightProducer}
 * still convert whatever this returns into a local {@code java.nio.file.Path}
 * at the point of use — so a remote URI can now be stored correctly, but
 * nothing downstream dispatches to the Hadoop-backed engine based on it
 * yet. That wiring is the next integration step, not done here.
 */
public final class TableCatalog {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern URI_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.*");

    private final Path catalogFile;
    private final Map<String, String> entries = new ConcurrentHashMap<>();

    public TableCatalog(Path catalogFile) {
        this.catalogFile = catalogFile;
        load();
    }

    /** Default location: {@code ~/.labukdb/catalog.json}. */
    public static TableCatalog atDefaultLocation() {
        Path defaultFile = Path.of(System.getProperty("user.home"), ".labukdb", "catalog.json");
        return new TableCatalog(defaultFile);
    }

    /** @param location a local path or a URI (e.g. {@code s3a://bucket/table}) — never coerced into a java.nio.file.Path here. */
    public synchronized void register(String name, String location) {
        entries.put(name, normalize(location));
        save();
    }

    public synchronized boolean unregister(String name) {
        boolean existed = entries.remove(name) != null;
        if (existed) {
            save();
        }
        return existed;
    }

    public Optional<String> resolve(String name) {
        return Optional.ofNullable(entries.get(name));
    }

    /** Sorted by name — stable, predictable output for a CLI listing or ListFlights. */
    public Map<String, String> listAll() {
        return new TreeMap<>(entries);
    }

    public static boolean isRemoteUri(String location) {
        return URI_SCHEME.matcher(location).matches();
    }

    private static String normalize(String location) {
        if (isRemoteUri(location)) {
            return location; // left exactly as given — normalizing as a local path would corrupt it
        }
        return Path.of(location).toAbsolutePath().normalize().toString();
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
