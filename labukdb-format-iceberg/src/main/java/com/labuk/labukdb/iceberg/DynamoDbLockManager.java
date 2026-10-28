package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.storage.DynamoDbLockConfiguration;
import org.apache.iceberg.LockManager;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.io.UncheckedIOException;
import java.io.IOException;
import java.util.Map;

/**
 * A real, cross-process {@link LockManager} for Iceberg's path-based
 * {@code HadoopTables} writer — the fix for a confirmed, root-caused gap:
 * {@code HadoopTables}' default lock manager
 * ({@code LockManagers.InMemoryLockManager}, confirmed via bytecode) is a
 * plain {@code HashMap} living inside one JVM's memory, giving zero
 * protection across genuinely separate writer processes. A real
 * 8-separate-process stress test against MinIO lost 6 of 8 commits
 * silently as a direct result (see {@code IcebergTableWriter}'s own
 * javadoc for the full root cause, traced through
 * {@code HadoopTableOperations.renameToFinal}).
 *
 * <b>Deliberately smaller than Delta's {@code LockProvider}.</b> Iceberg's
 * own {@code SnapshotProducer.commit()} already retries on {@code
 * CommitFailedException} by re-resolving fresh metadata and trying again
 * (confirmed via bytecode) — this class only needs to fail fast and
 * return {@code false} on real contention; no crash-recovery design is
 * needed the way Delta's temp-file-plus-entry dance required, since
 * Iceberg has no equivalent "temp content for someone else to finish"
 * step. A simple lease: {@link #acquire} steals an expired lock
 * (idiomatic DynamoDB distributed-lock pattern:
 * {@code attribute_not_exists(lockKey) OR expiresAt < :now}), not a
 * Delta-style external-entry recovery.
 *
 * <b>Activated via Iceberg's own existing extension point</b> — confirmed
 * via {@code javap -constants} on {@code HadoopTables}, not assumed:
 * {@code HadoopTables.createOrGetLockManager} reads Hadoop {@code
 * Configuration} keys prefixed {@code iceberg.tables.hadoop.}, and if the
 * stripped map contains {@code lock-impl}, reflectively instantiates that
 * class and calls {@link #initialize}. See {@link IcebergLockManagerConfig}
 * for where that property actually gets set — opt-in, same philosophy as
 * Delta's {@code LockProvider}.
 */
public final class DynamoDbLockManager implements LockManager {

    private static final String ATTR_LOCK_KEY = "lockKey";
    private static final String ATTR_OWNER = "owner";
    private static final String ATTR_EXPIRES_AT = "expiresAt";

    static final String TABLE_NAME_ENV_VAR = "ICEBERG_LOCK_DYNAMODB_TABLE";
    static final String LEASE_SECONDS_ENV_VAR = "ICEBERG_LOCK_LEASE_SECONDS";
    static final long DEFAULT_LEASE_SECONDS = 60; // short: a stuck lock blocks the very next writer, unlike Delta's 24h entry TTL

    private DynamoDbClient client;
    private String tableName;
    private long leaseSeconds;

    @Override
    public void initialize(Map<String, String> properties) {
        DynamoDbLockConfiguration config = DynamoDbLockConfiguration
                .fromEnvironment(TABLE_NAME_ENV_VAR, LEASE_SECONDS_ENV_VAR, DEFAULT_LEASE_SECONDS)
                .orElseThrow(() -> new IllegalStateException(
                        TABLE_NAME_ENV_VAR + " must be set to activate the Iceberg DynamoDB lock"));

        this.client = config.buildClient();
        this.tableName = config.tableName();
        this.leaseSeconds = config.expirySeconds();
        ensureTableExists();
    }

    private void ensureTableExists() {
        try {
            TableStatus status = client.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build()).table().tableStatus();
            if (status == TableStatus.ACTIVE) {
                return;
            }
        } catch (ResourceNotFoundException notYetCreated) {
            client.createTable(r -> r.tableName(tableName)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .keySchema(KeySchemaElement.builder().attributeName(ATTR_LOCK_KEY).keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName(ATTR_LOCK_KEY).attributeType(ScalarAttributeType.S).build()));
        }

        for (int attempt = 0; attempt < 25; attempt++) {
            TableStatus status = client.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build()).table().tableStatus();
            if (status == TableStatus.ACTIVE) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedIOException(new IOException("Interrupted waiting for lock table " + tableName + " to become ACTIVE"));
            }
        }
        throw new UncheckedIOException(new IOException("Lock table " + tableName + " never became ACTIVE"));
    }

    /**
     * {@code attribute_not_exists(lockKey) OR expiresAt < :now} — the real
     * mutual-exclusion point. Succeeds either on a genuinely free key, or
     * on reclaiming a lease whose holder never released it in time (the
     * accepted tradeoff for not building Delta-style crash recovery here).
     * No internal retry: {@code SnapshotProducer.commit()}'s own retry
     * loop is what re-attempts on {@code false}.
     */
    @Override
    public boolean acquire(String entityId, String ownerId) {
        long now = System.currentTimeMillis() / 1000;
        long expiresAt = now + leaseSeconds;
        try {
            client.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of(
                            ATTR_LOCK_KEY, AttributeValue.builder().s(entityId).build(),
                            ATTR_OWNER, AttributeValue.builder().s(ownerId).build(),
                            ATTR_EXPIRES_AT, AttributeValue.builder().n(String.valueOf(expiresAt)).build()))
                    .conditionExpression("attribute_not_exists(" + ATTR_LOCK_KEY + ") OR " + ATTR_EXPIRES_AT + " < :now")
                    .expressionAttributeValues(Map.of(":now", AttributeValue.builder().n(String.valueOf(now)).build()))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException stillHeld) {
            return false;
        }
    }

    /**
     * {@code #owner = :owner} — only the actual holder can release, so a
     * lease-expiry steal by someone else can't be clobbered by a late
     * release. {@code owner} is a DynamoDB reserved keyword (confirmed the
     * hard way — {@code DynamoDbException: Attribute name is a reserved
     * keyword}, not assumed) — {@code #owner} is an {@code
     * ExpressionAttributeNames} placeholder, the standard way around this.
     */
    @Override
    public boolean release(String entityId, String ownerId) {
        try {
            client.deleteItem(DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of(ATTR_LOCK_KEY, AttributeValue.builder().s(entityId).build()))
                    .conditionExpression("#owner = :owner")
                    .expressionAttributeNames(Map.of("#owner", ATTR_OWNER))
                    .expressionAttributeValues(Map.of(":owner", AttributeValue.builder().s(ownerId).build()))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException notTheHolder) {
            return false;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
