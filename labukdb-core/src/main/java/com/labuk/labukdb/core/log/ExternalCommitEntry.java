package com.labuk.labukdb.core.log;

/**
 * One row of a {@link LockProvider}'s backing store — the external record
 * of a single commit-in-progress, mirroring the real schema
 * {@code io.delta.storage.S3DynamoDBLogStore} uses (confirmed from its real
 * source, not invented): a composite key of {@code tablePath} + {@code
 * fileName}, a pointer to the temp file holding the commit's actual content
 * so a different process can finish an abandoned commit, a completion flag,
 * and an optional TTL.
 *
 * Never JSON-serialized onto disk (unlike {@code SingleAction} and friends)
 * — it only ever travels through a {@link LockProvider} implementation's own
 * backing store (e.g. a DynamoDB item), so a plain record is enough.
 */
public record ExternalCommitEntry(String tablePath, String fileName, String tempPath,
                                   boolean complete, Long expireTime) {

    /** The same entry, marked complete with a TTL {@code expireAfterSeconds} from now. */
    public ExternalCommitEntry asComplete(long expireAfterSeconds) {
        return new ExternalCommitEntry(tablePath, fileName, tempPath, true,
                System.currentTimeMillis() / 1000 + expireAfterSeconds);
    }
}
