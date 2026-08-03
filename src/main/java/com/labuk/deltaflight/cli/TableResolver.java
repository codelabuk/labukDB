package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.catalog.TableCatalog;
import com.labuk.deltaflight.storage.MinioConfiguration;
import com.labuk.deltaflight.storage.TableLocation;

/**
 * Resolves a CLI argument that could be a catalog name, a local path, or a
 * remote URI - the same "works with all of them" intuition {@code kubectl
 * get pod <name>} relies on. Catalog lookup wins if it matches; otherwise
 * the argument is used literally.
 *
 * Returns a {@link TableLocation}, so remote entries actually dispatch to
 * the Hadoop-backed engine rather than being flattened into a broken local
 * path (which is what this class did before URI dispatch existed).
 * Remote locations pick up connector config from the environment via
 * {@link MinioConfiguration#fromEnvironment()} - set {@code MINIO_ENDPOINT},
 * {@code MINIO_ACCESS_KEY}, {@code MINIO_SECRET_KEY} to query MinIO from
 * the CLI.
 */
final class TableResolver {

    private TableResolver() {
    }

    static TableLocation resolve(String tableArg, TableCatalog catalog) {
        String raw = catalog.resolve(tableArg).orElse(tableArg);
        return TableLocation.isRemoteUri(raw)
                ? TableLocation.forRemote(raw, MinioConfiguration.fromEnvironment())
                : TableLocation.fromString(raw);
    }
}
