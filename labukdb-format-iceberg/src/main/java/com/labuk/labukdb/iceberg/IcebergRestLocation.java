package com.labuk.labukdb.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Recognizes and parses an {@code icebergrest://<namespace>.<table>}
 * table reference — deliberately a separate scheme from
 * {@code TableLocation}'s local/remote path model, not a third
 * {@code TableLocation} variant: a REST-catalog-addressed table has no
 * filesystem path to probe before the catalog has already resolved it
 * (unlike {@code DetectedFormat}'s {@code _delta_log}/{@code metadata}
 * directory check), so it can't share that contract.
 *
 * <b>A real, confirmed footgun this exists to avoid:</b> {@code
 * TableLocation.isRemoteUri}'s generic {@code scheme://} regex would
 * match {@code icebergrest://} too, so without checking for it first, a
 * caller would hand it to Hadoop's {@code FileSystem} resolution and get
 * a confusing "No FileSystem for scheme" error instead of routing it
 * correctly. Callers must check {@link #isRestUri} <b>before</b>
 * constructing any {@code TableLocation}.
 *
 * Deliberately a separate catalog connection from the table identifier
 * (not one combined URI) — see {@link IcebergRestConfiguration}'s
 * javadoc for why; the table identifier itself reuses {@code
 * iceberg-core}'s own {@link TableIdentifier#parse(String)}, which
 * already handles dotted multi-level namespace syntax natively.
 */
public final class IcebergRestLocation {

    private static final String SCHEME = "icebergrest://";

    private IcebergRestLocation() {
    }

    public static boolean isRestUri(String raw) {
        return raw != null && raw.startsWith(SCHEME);
    }

    /** @param raw must satisfy {@link #isRestUri}; e.g. {@code "icebergrest://analytics.logs"}. */
    public static TableIdentifier parseTableIdentifier(String raw) {
        if (!isRestUri(raw)) {
            throw new IllegalArgumentException("Not an icebergrest:// table reference: " + raw);
        }
        return TableIdentifier.parse(raw.substring(SCHEME.length()));
    }
}
