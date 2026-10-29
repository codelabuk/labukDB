package com.labuk.labukdb.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proportionate logged warning for the one configuration this project has
 * confirmed is unsafe: a remote writer with no external lock engaged. Both
 * {@code log.CommitWriter} and {@code iceberg.IcebergTableWriter} call this
 * from their no-lock, path-based remote construction path — see each
 * class's own javadoc for the real, root-caused gap this refers to
 * ({@code CommitWriterConcurrentMinioIntegrationTest} and
 * {@code IcebergTableWriterConcurrentMinioIntegrationTest}, both standing
 * proof tests that fail on purpose).
 *
 * <b>Deliberately proportionate to backend risk, not a blanket refusal —
 * this engine has no way to detect an actual concurrent writer</b> (that
 * would require the lock it doesn't have), only risky *configuration*:
 * <ul>
 *   <li>An explicit {@code fs.s3a.endpoint} (set by {@link MinioConfiguration}
 *       for any self-hosted/third-party S3-compatible store) gets a strong
 *       warning — this is the exact shape of deployment a live diagnostic
 *       already confirmed loses commits silently under genuine concurrency.</li>
 *   <li>Any other remote location (default AWS credential-chain
 *       {@code s3a://}, or {@code abfs://}/{@code abfss://} ADLS) gets a
 *       lighter informational note — real S3/ADLS/GCS are documented as
 *       reliable for conditional writes, but this project has not
 *       independently re-verified that the way it did the MinIO failure
 *       (see {@code LockProvider}'s own javadoc for this exact distinction).</li>
 *   <li>Local tables, and any remote table that already has a lock engaged,
 *       produce no warning at all.</li>
 * </ul>
 */
public final class RemoteWriteSafetyWarning {

    private static final Logger log = LoggerFactory.getLogger(RemoteWriteSafetyWarning.class);

    private RemoteWriteSafetyWarning() {
    }

    /**
     * @param location      the writer's table location
     * @param lockConfigured whether this writer engaged a real external lock
     *                       (a {@code LockProvider} for Delta, a configured
     *                       {@code DynamoDbLockManager} for Iceberg)
     * @param formatName    {@code "Delta"} or {@code "Iceberg"}, for the message only
     */
    public static void warnIfUnsafe(TableLocation location, boolean lockConfigured, String formatName) {
        if (!location.isRemote() || lockConfigured) {
            return;
        }

        String endpoint = location.hadoopConf().get("fs.s3a.endpoint");
        if (endpoint != null && !endpoint.isBlank()) {
            log.warn("{} table at {} is on S3-compatible storage with an explicit custom endpoint "
                            + "({}) and no write lock configured. This project has confirmed, "
                            + "root-caused silent commit loss under genuinely concurrent writers "
                            + "against at least one such deployment (MinIO). If more than one "
                            + "process may write to this table concurrently, configure a lock "
                            + "(DynamoDbLockProvider for Delta, ICEBERG_LOCK_DYNAMODB_TABLE for "
                            + "Iceberg) before relying on this table's correctness under concurrency.",
                    formatName, location.raw(), endpoint);
        } else {
            log.info("{} table at {} is on remote storage with no write lock configured. This is "
                            + "expected to be safe for a single writer, and documented (not "
                            + "independently re-verified by this project) as safe for concurrent "
                            + "writers on real AWS S3/Azure ADLS/GCS. If this table may see "
                            + "genuinely concurrent writers on storage you haven't verified, the "
                            + "same lock mechanism proven against MinIO can still be configured.",
                    formatName, location.raw());
        }
    }
}
