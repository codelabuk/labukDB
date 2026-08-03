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
 *
 * <b>Interim limitation:</b> this always returns a local
 * {@code java.nio.file.Path}, even if the catalog entry is a remote URI
 * (which {@link TableCatalog} can now correctly store). The CLI doesn't
 * yet dispatch to the Hadoop-backed engine for remote entries — that's
 * the next integration step, not this class's job yet. Resolving a
 * remote-URI catalog entry through the CLI today will produce a broken
 * local Path, not a working remote query.
 */
final class TableResolver {

    private TableResolver() {
    }

    static Path resolve(String tableArg, TableCatalog catalog) {
        return catalog.resolve(tableArg).map(Paths::get).orElseGet(() -> Paths.get(tableArg));
    }
}
