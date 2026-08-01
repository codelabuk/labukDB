package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.catalog.TableCatalog;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resolves a CLI argument that could be either a name registered in the
 * catalog or a raw filesystem path — the same "works with both" intuition
 * {@code kubectl get pod <name>} relies on. Catalog lookup wins if it
 * matches; otherwise the argument is treated as a literal path, so nothing
 * about pre-catalog usage (a raw path) breaks.
 */
final class TableResolver {

    private TableResolver() {
    }

    static Path resolve(String tableArg, TableCatalog catalog) {
        return catalog.resolve(tableArg).orElseGet(() -> Paths.get(tableArg));
    }
}
