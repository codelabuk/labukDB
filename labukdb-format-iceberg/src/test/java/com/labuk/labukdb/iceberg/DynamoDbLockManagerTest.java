package com.labuk.labukdb.iceberg;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves {@link DynamoDbLockManager} against a real DynamoDB backend —
 * either real AWS DynamoDB or a self-hosted {@code amazon/dynamodb-local}
 * container, same convention as every other live-infra test in this
 * project. Opt-in: skips (not fails) when {@code ICEBERG_LOCK_DYNAMODB_TABLE}
 * isn't set.
 *
 * Reads {@code ICEBERG_LOCK_DYNAMODB_TABLE}, {@code DYNAMODB_ENDPOINT}
 * (set for {@code amazon/dynamodb-local}), {@code DYNAMODB_ACCESS_KEY}/
 * {@code DYNAMODB_SECRET_KEY}, and {@code ICEBERG_LOCK_LEASE_SECONDS}
 * (use a short value, e.g. {@code 2}, so the lease-expiry test below
 * doesn't need a long real sleep).
 */
class DynamoDbLockManagerTest {

    private DynamoDbLockManager lockManager;

    @BeforeEach
    void checkPreconditionsAndInitialize() {
        assumeTrue(System.getenv("ICEBERG_LOCK_DYNAMODB_TABLE") != null,
                "ICEBERG_LOCK_DYNAMODB_TABLE not set — skipping live Iceberg DynamoDB lock test");
        lockManager = new DynamoDbLockManager();
        lockManager.initialize(Map.of());
    }

    @AfterEach
    void cleanUp() {
        if (lockManager != null) {
            lockManager.close();
        }
    }

    @Test
    void acquire_freeKey_succeeds() {
        assertTrue(lockManager.acquire("entity-" + UUID.randomUUID(), "owner-a"));
    }

    @Test
    void acquire_alreadyHeldKey_fails() {
        String entityId = "entity-" + UUID.randomUUID();
        assertTrue(lockManager.acquire(entityId, "owner-a"));
        assertFalse(lockManager.acquire(entityId, "owner-b"), "a second acquire before expiry must fail");
    }

    @Test
    void release_thenReacquire_succeeds() {
        String entityId = "entity-" + UUID.randomUUID();
        assertTrue(lockManager.acquire(entityId, "owner-a"));
        assertTrue(lockManager.release(entityId, "owner-a"));
        assertTrue(lockManager.acquire(entityId, "owner-b"), "after a real release, a different owner must be able to acquire");
    }

    @Test
    void release_byWrongOwner_fails() {
        String entityId = "entity-" + UUID.randomUUID();
        assertTrue(lockManager.acquire(entityId, "owner-a"));
        assertFalse(lockManager.release(entityId, "owner-b"), "only the actual holder may release");
    }

    /**
     * The real "steal" case this lock is scoped to, instead of Delta-style
     * crash recovery: a short lease expires on its own, and a later writer
     * can acquire without anyone ever calling release.
     */
    @Test
    void acquire_afterLeaseExpiresWithoutRelease_succeeds() throws Exception {
        String leaseRaw = System.getenv("ICEBERG_LOCK_LEASE_SECONDS");
        long leaseSeconds = (leaseRaw == null || leaseRaw.isBlank()) ? DynamoDbLockManager.DEFAULT_LEASE_SECONDS
                : Long.parseLong(leaseRaw.trim());
        assumeTrue(leaseSeconds <= 3, "set ICEBERG_LOCK_LEASE_SECONDS to a small value (e.g. 2) to run this test quickly");

        String entityId = "entity-" + UUID.randomUUID();
        assertTrue(lockManager.acquire(entityId, "owner-a"));
        Thread.sleep((leaseSeconds + 1) * 1000);
        assertTrue(lockManager.acquire(entityId, "owner-b"), "a different owner must be able to steal an expired lease");
    }
}
