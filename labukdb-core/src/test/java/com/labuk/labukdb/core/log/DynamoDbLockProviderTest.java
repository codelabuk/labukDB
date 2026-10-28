package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.DynamoDbLockConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves {@link DynamoDbLockProvider} against a real DynamoDB backend —
 * either real AWS DynamoDB or (what this project's own development and the
 * live diagnostic behind this whole feature used) a self-hosted {@code
 * amazon/dynamodb-local} container, AWS's own official, no-account-needed
 * emulator. Opt-in by design, same env-var-gated {@code assumeTrue} skip
 * convention as every MinIO integration test in this project.
 *
 * Reads {@code DYNAMODB_LOCK_TABLE}, {@code DYNAMODB_ENDPOINT} (set this for
 * {@code amazon/dynamodb-local}, e.g. {@code http://localhost:8000}),
 * {@code DYNAMODB_ACCESS_KEY}/{@code DYNAMODB_SECRET_KEY} (DynamoDB Local's
 * fake static credentials; omit for real AWS).
 */
class DynamoDbLockProviderTest {

    private DynamoDbLockProvider provider;
    private String tablePath;

    @BeforeEach
    void checkPreconditionsAndConnect() {
        Optional<DynamoDbLockConfiguration> config = DynamoDbLockConfiguration.fromEnvironment();
        assumeTrue(config.isPresent(), "DYNAMODB_LOCK_TABLE not set — skipping live DynamoDB lock test");

        provider = DynamoDbLockProvider.fromConfiguration(config.get());
        tablePath = "labukdb-lock-test-table-" + UUID.randomUUID();
    }

    // No explicit cleanup: each test uses a fresh random tablePath, so leftover
    // items never collide with a future run; TTL (where enforced) reaps them.

    @Test
    void putEntry_withoutOverwrite_conflictsOnDuplicateKey() throws Exception {
        String fileName = CommitFileNames.jsonFileName(1);
        ExternalCommitEntry entry = new ExternalCommitEntry(tablePath, fileName, ".tmp/a.json", false, null);
        provider.putEntry(entry, false);

        ExternalCommitEntry conflicting = new ExternalCommitEntry(tablePath, fileName, ".tmp/b.json", false, null);
        assertThrows(FileAlreadyExistsException.class, () -> provider.putEntry(conflicting, false));
    }

    @Test
    void putEntry_withOverwrite_alwaysSucceeds() throws Exception {
        String fileName = CommitFileNames.jsonFileName(1);
        ExternalCommitEntry entry = new ExternalCommitEntry(tablePath, fileName, ".tmp/a.json", false, null);
        provider.putEntry(entry, false);

        ExternalCommitEntry completed = entry.asComplete(3600);
        provider.putEntry(completed, true);

        ExternalCommitEntry readBack = provider.getEntry(tablePath, fileName).orElseThrow();
        assertTrue(readBack.complete());
        assertNotNull(readBack.expireTime());
    }

    @Test
    void getEntry_roundTripsAllFields() throws Exception {
        String fileName = CommitFileNames.jsonFileName(5);
        ExternalCommitEntry entry = new ExternalCommitEntry(tablePath, fileName, ".tmp/round-trip.json", false, null);
        provider.putEntry(entry, false);

        ExternalCommitEntry readBack = provider.getEntry(tablePath, fileName).orElseThrow();
        assertEquals(tablePath, readBack.tablePath());
        assertEquals(fileName, readBack.fileName());
        assertEquals(".tmp/round-trip.json", readBack.tempPath());
        assertFalse(readBack.complete());
    }

    @Test
    void getEntry_missingKey_returnsEmpty() throws Exception {
        assertTrue(provider.getEntry(tablePath, CommitFileNames.jsonFileName(99)).isEmpty());
    }

    /**
     * The actual real-concurrency proof — the same methodology as the raw
     * S3A diagnostic that found MinIO's gap in the first place, run against
     * DynamoDB instead: 8 threads, identical key, no retry logic involved.
     * This is what justified picking DynamoDB over trusting another
     * conditional-write primitive without verifying it first.
     */
    @Test
    void putEntry_concurrentWritersSameKey_exactlyOneWins() throws Exception {
        String fileName = CommitFileNames.jsonFileName(1);
        int threadCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            int id = i;
            futures.add(pool.submit(() -> {
                startGate.await();
                ExternalCommitEntry entry = new ExternalCommitEntry(tablePath, fileName, ".tmp/writer-" + id + ".json", false, null);
                try {
                    provider.putEntry(entry, false);
                    successCount.incrementAndGet();
                } catch (FileAlreadyExistsException conflict) {
                    conflictCount.incrementAndGet();
                }
                return null;
            }));
        }

        startGate.countDown();
        for (Future<Void> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(1, successCount.get(), "exactly one writer should win the conditional put");
        assertEquals(threadCount - 1, conflictCount.get(), "every other writer should get a clean conflict");
    }
}
