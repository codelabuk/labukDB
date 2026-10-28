package com.labuk.labukdb.core.log;

import java.io.IOException;
import java.util.Optional;

/**
 * An out-of-band mutual-exclusion primitive for {@link CommitWriter}'s
 * remote commit path.
 *
 * <b>Scope, stated precisely — a mitigation for non-compliant S3-compatible
 * object storage, not a universal multi-cloud requirement.</b> The gap this
 * exists for is narrower than "object storage can't do atomic writes": a
 * live diagnostic proved one specific real MinIO deployment does not
 * honor S3's conditional-create contract under genuine concurrency (see
 * {@code CommitWriterConcurrentMinioIntegrationTest}, which fails on purpose
 * as a standing proof) — but the *first-party* implementations this feature
 * was actually designed against are documented as reliable: real AWS S3
 * (conditional writes since late 2024), real Google Cloud Storage
 * (generation-precondition creates, strongly consistent for years, per
 * Google's own docs), and real Azure Blob Storage/ADLS (`If-None-Match: *`,
 * a long-standing native REST feature) are all very likely fine without any
 * external lock at all. Matches Hadoop's own HADOOP-19256 release notes,
 * which explicitly flag third-party S3-compatible re-implementations, not
 * S3 itself, as the unverified case. <b>This hasn't been empirically
 * re-tested the way the MinIO failure was</b> — treat "real S3/ADLS/GCS
 * don't need this" as a documented, not yet independently verified, claim.
 *
 * Delta's own on-disk log format can't be restructured around a mutable-
 * pointer compare-and-swap trick (every reader depends on one immutable
 * file per version), so for storage that genuinely can't be trusted, the
 * fix has to be a real external lock. The one shipped implementation,
 * {@link DynamoDbLockProvider}, is a reasonable default specifically for
 * this self-hosted/non-compliant-storage case — it works against a
 * self-hostable {@code amazon/dynamodb-local} container just as well as
 * real AWS DynamoDB, so it doesn't require an AWS account or tie a
 * self-hosted MinIO deployment to AWS. A real-cloud deployment on genuine
 * S3/ADLS/GCS should likely never need to configure a {@link LockProvider}
 * at all.
 *
 * <b>Opt-in, not mandatory</b> — {@link CommitWriter#forLocation(com.labuk.labukdb.core.storage.TableLocation)}
 * (no lock) remains the default, already proven safe for a single remote
 * writer. A {@link LockProvider} only engages when a caller explicitly
 * supplies one via {@link CommitWriter#forLocation(com.labuk.labukdb.core.storage.TableLocation, LockProvider)}.
 *
 * <b>Every writer of a given table must agree</b> on using the same
 * {@link LockProvider} (same backing table), or none at all — one writer
 * bypassing the lock while another uses it silently defeats the whole
 * scheme for that table, since the bypassing writer races the real file
 * directly with no external coordination. Real Delta Lake's own
 * multi-cluster docs (the closest prior art for this exact mechanism, a
 * table-format library rather than a full database, but the same narrow
 * problem) carry this identical caveat.
 */
public interface LockProvider {

    /**
     * Writes {@code entry} keyed by {@code (entry.tablePath(), entry.fileName())}.
     *
     * @param overwrite when {@code false}, this is the real mutual-exclusion
     *                  point: must throw {@link java.nio.file.FileAlreadyExistsException}
     *                  if an entry already exists for that key, rather than
     *                  silently overwriting it — the same "atomic create
     *                  lost the race" convention {@link CommitWriter}
     *                  already uses for local/Hadoop file creation, reused
     *                  here deliberately so the existing retry loop in
     *                  {@link CommitWriter#appendBatch} needs no new catch
     *                  clause. When {@code true}, always succeeds (used to
     *                  mark an entry complete).
     */
    void putEntry(ExternalCommitEntry entry, boolean overwrite) throws IOException;

    /** The entry for {@code (tablePath, fileName)}, if one exists. Must be a consistent (not stale) read. */
    Optional<ExternalCommitEntry> getEntry(String tablePath, String fileName) throws IOException;
}
