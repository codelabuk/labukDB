package com.labuk.labukdb.core.storage;

/**
 * Thrown by {@link RemoteWriteSafetyWarning#enforce} when a writer is being
 * constructed against a remote location this project has <b>confirmed</b>
 * loses commits silently under genuine concurrent writers (an explicit
 * S3-compatible custom endpoint, e.g. MinIO — see that method's javadoc for
 * the exact condition) with no external lock engaged and no explicit
 * single-writer acknowledgment either.
 *
 * Fail-closed by design: this configuration used to only log a warning and
 * proceed, which meant the confirmed-broken multi-writer case and the
 * documented-safe single-writer case looked identical at every call site. A
 * caller now has to pick one of two explicit paths — engage a real lock
 * ({@code CommitWriter.forLocation(location, LockProvider)}, Delta; a
 * configured {@code DynamoDbLockManager}, Iceberg), or state out loud that
 * this table will only ever have one writer
 * ({@code CommitWriter.forLocationSingleWriterOnly(location)} /
 * {@code IcebergTableWriter.forLocationSingleWriterOnly(location)}).
 */
public final class UnsafeRemoteWriteException extends RuntimeException {

    public UnsafeRemoteWriteException(String message) {
        super(message);
    }
}
