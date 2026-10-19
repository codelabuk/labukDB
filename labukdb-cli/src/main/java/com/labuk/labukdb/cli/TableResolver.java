package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.storage.AdlsConfiguration;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.hadoop.conf.Configuration;

/**
 * Resolves a CLI argument that could be a catalog name, a local path, or a
 * remote URI - the same "works with all of them" intuition {@code kubectl
 * get pod <name>} relies on. Catalog lookup wins if it matches; otherwise
 * the argument is used literally.
 *
 * Remote locations pick up both MinIO and ADLS connector config from the
 * environment via their respective {@code fromEnvironment()} helpers.
 * Set the relevant env vars for whichever backend you're connecting to:
 *
 * MinIO/S3: {@code MINIO_ENDPOINT}, {@code MINIO_ACCESS_KEY}, {@code MINIO_SECRET_KEY}
 * ADLS Gen2: {@code ADLS_STORAGE_ACCOUNT}, {@code ADLS_AUTH_TYPE}, plus key or OAuth vars
 */
final class TableResolver {

    private TableResolver() {
    }

    static TableLocation resolve(String tableArg, TableCatalog catalog) {
        String raw = catalog.resolve(tableArg).orElse(tableArg);
        if (TableLocation.isRemoteUri(raw)) {
            Configuration conf = AdlsConfiguration.fromEnvironment();
            MinioConfiguration.fromEnvironment().iterator()
                    .forEachRemaining(entry -> conf.set(entry.getKey(), entry.getValue()));
            return TableLocation.forRemote(raw, conf);
        }
        return TableLocation.fromString(raw);
    }
}
