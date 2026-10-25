package com.labuk.labukdb.core.storage;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Where a table lives — local disk or remote object storage — resolved
 * once, at the boundary where a location string enters the system, so
 * every layer below can ask "local or remote?" without each one
 * re-implementing URI detection.
 *
 * <b>This exists to close the last real storage gap.</b> The log layer
 * ({@code LogStorage}) and decode layer ({@code ParquetInputFiles}) were
 * both made remote-capable separately, but nothing actually decided which
 * to use — every caller hardcoded the local path. This type is that
 * decision, made once.
 *
 * <b>Credentials/config:</b> the Hadoop {@link Configuration} carried here
 * is what a connector needs (endpoint, access key, path-style access for
 * MinIO, etc.). {@link #forRemote} takes one so a caller can supply
 * deployment-specific settings; {@link #fromString} builds a default one,
 * which works when config comes from environment/Hadoop's own
 * {@code core-site.xml} discovery rather than being set programmatically.
 * See {@code MinioConfiguration} for a concrete MinIO-shaped helper.
 */
public final class TableLocation {

    private static final Pattern URI_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.*");

    private final boolean remote;
    private final String raw;
    private final Configuration conf;

    private TableLocation(boolean remote, String raw, Configuration conf) {
        this.remote = remote;
        this.raw = raw;
        this.conf = conf;
    }

    /** Detects scheme automatically; remote locations get a default Hadoop Configuration. */
    public static TableLocation fromString(String location) {
        return isRemoteUri(location)
                ? new TableLocation(true, location, new Configuration())
                : new TableLocation(false, location, null);
    }

    /** Explicit remote, with caller-supplied connector configuration (endpoint, credentials, ...). */
    public static TableLocation forRemote(String uri, Configuration conf) {
        if (!isRemoteUri(uri)) {
            throw new IllegalArgumentException("Not a remote URI (expected scheme://...): " + uri);
        }
        return new TableLocation(true, uri, conf);
    }

    public static TableLocation forLocal(Path path) {
        return new TableLocation(false, path.toString(), null);
    }

    public static boolean isRemoteUri(String location) {
        return location != null && URI_SCHEME.matcher(location).matches();
    }

    public boolean isRemote() {
        return remote;
    }

    public String raw() {
        return raw;
    }

    /** Only valid when {@link #isRemote()} is false. */
    public Path localPath() {
        if (remote) {
            throw new IllegalStateException("Not a local location: " + raw);
        }
        return Paths.get(raw);
    }

    /** Only valid when {@link #isRemote()} is true. */
    public org.apache.hadoop.fs.Path hadoopPath() {
        if (!remote) {
            throw new IllegalStateException("Not a remote location: " + raw);
        }
        return new org.apache.hadoop.fs.Path(raw);
    }

    /** Only valid when {@link #isRemote()} is true. */
    public Configuration hadoopConf() {
        if (!remote) {
            throw new IllegalStateException("Not a remote location: " + raw);
        }
        return conf;
    }

    /** Resolves a relative path (e.g. an AddFile's path) against this location, preserving local/remote-ness. */
    public TableLocation resolve(String relativePath) {
        if (remote) {
            return new TableLocation(true, new org.apache.hadoop.fs.Path(hadoopPath(), relativePath).toString(), conf);
        }
        return new TableLocation(false, localPath().resolve(relativePath).toString(), null);
    }

    /**
     * Whether a path relative to this location exists — the generic FS
     * primitive {@code format.DetectedFormat} probes with (e.g.
     * {@code "_delta_log"} vs. {@code "metadata"}) to tell a Delta table
     * from an Iceberg one before committing to either read path.
     */
    public boolean exists(String relativePath) throws IOException {
        if (remote) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(hadoopPath(), relativePath);
            return path.getFileSystem(conf).exists(path);
        }
        return Files.exists(localPath().resolve(relativePath));
    }

    @Override
    public String toString() {
        return raw;
    }
}
