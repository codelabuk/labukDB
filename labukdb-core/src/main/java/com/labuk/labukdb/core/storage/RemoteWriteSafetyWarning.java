package com.labuk.labukdb.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fail-closed enforcement for the one configuration this project has
 * confirmed is unsafe: a remote writer, on storage that already
 * demonstrated silent commit loss under real concurrency, with no external
 * lock engaged. Both {@code log.CommitWriter} and
 * {@code iceberg.IcebergTableWriter} call this from their no-lock,
 * path-based remote construction path — see each class's own javadoc for
 * the real, root-caused gap this refers to
 * ({@code CommitWriterConcurrentMinioIntegrationTest} and
 * {@code IcebergTableWriterConcurrentMinioIntegrationTest}, both standing
 * proof tests that fail on purpose).
 *
 * <b>Deliberately proportionate to backend risk, not a blanket refusal —
 * this engine has no way to detect an actual concurrent writer</b> (that
 * would require the lock it doesn't have), only risky *configuration*. That
 * is exactly why this used to only log: a silently-safe single writer and a
 * silently-broken concurrent one look identical from here. Logging alone
 * meant that distinction could be missed entirely, so the confirmed-unsafe
 * tier below now requires an explicit decision instead of defaulting past
 * it:
 * <ul>
 *   <li>An explicit {@code fs.s3a.endpoint} (set by {@link MinioConfiguration}
 *       for any self-hosted/third-party S3-compatible store) with no lock and
 *       no acknowledgment throws {@link UnsafeRemoteWriteException} — this is
 *       the exact shape of deployment a live diagnostic already confirmed
 *       loses commits silently under genuine concurrency. Passing
 *       {@code acknowledgedSingleWriterRisk=true} (via
 *       {@code CommitWriter.forLocationSingleWriterOnly}/
 *       {@code IcebergTableWriter.forLocationSingleWriterOnly}) downgrades
 *       this to the same warning the constructor used to log unconditionally.</li>
 *   <li>Any other remote location (default AWS credential-chain
 *       {@code s3a://}, or {@code abfs://}/{@code abfss://} ADLS) gets a
 *       lighter informational note, unconditionally — real S3/ADLS/GCS are
 *       documented as reliable for conditional writes, but this project has
 *       not independently re-verified that the way it did the MinIO failure
 *       (see {@code LockProvider}'s own javadoc for this exact distinction),
 *       so this tier was never promoted to a hard failure.</li>
 *   <li>Local tables, and any remote table that already has a lock engaged,
 *       produce no warning and never throw.</li>
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
     * @param acknowledgedSingleWriterRisk whether the caller explicitly
     *                       opted into the confirmed-unsafe configuration via
     *                       a single-writer-only entry point; ignored when
     *                       {@code lockConfigured} is true or the location
     *                       isn't the confirmed-unsafe tier
     * @param formatName    {@code "Delta"} or {@code "Iceberg"}, for the message only
     * @throws UnsafeRemoteWriteException if {@code location} is remote, has
     *                       an explicit custom S3-compatible endpoint, has
     *                       no lock configured, and the risk wasn't
     *                       explicitly acknowledged
     */
    public static void enforce(TableLocation location, boolean lockConfigured,
                                boolean acknowledgedSingleWriterRisk, String formatName) {
        if (!location.isRemote() || lockConfigured) {
            return;
        }

        String endpoint = location.hadoopConf().get("fs.s3a.endpoint");
        if (endpoint != null && !endpoint.isBlank()) {
            if (!acknowledgedSingleWriterRisk) {
                throw new UnsafeRemoteWriteException(String.format(
                        "%s table at %s is on S3-compatible storage with an explicit custom endpoint "
                                + "(%s) and no write lock configured. This project has confirmed, "
                                + "root-caused silent commit loss under genuinely concurrent writers "
                                + "against at least one such deployment (MinIO), so this configuration "
                                + "now fails closed by default. Either configure a lock "
                                + "(DynamoDbLockProvider for Delta, ICEBERG_LOCK_DYNAMODB_TABLE for "
                                + "Iceberg), or, if this table will only ever have one writer, use the "
                                + "explicit single-writer-only entry point "
                                + "(CommitWriter.forLocationSingleWriterOnly / "
                                + "IcebergTableWriter.forLocationSingleWriterOnly) to state that out loud.",
                        formatName, location.raw(), endpoint));
            }
            log.warn("{} table at {} is on S3-compatible storage with an explicit custom endpoint "
                            + "({}) and no write lock configured. The single-writer risk was "
                            + "explicitly acknowledged, so proceeding — but this project has "
                            + "confirmed, root-caused silent commit loss under genuinely concurrent "
                            + "writers against at least one such deployment (MinIO). If more than "
                            + "one process may write to this table, configure a lock instead "
                            + "(DynamoDbLockProvider for Delta, ICEBERG_LOCK_DYNAMODB_TABLE for Iceberg).",
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
