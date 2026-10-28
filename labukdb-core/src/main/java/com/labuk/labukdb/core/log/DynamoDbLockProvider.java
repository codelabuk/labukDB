package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.DynamoDbLockConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link LockProvider} backed by a real DynamoDB table — same schema as
 * real Delta Lake OSS's own {@code io.delta.storage.S3DynamoDBLogStore}
 * (confirmed from its real source, not invented): HASH key {@code tablePath},
 * RANGE key {@code fileName}, a {@code tempPath} pointer, a {@code complete}
 * flag, and a TTL attribute {@code expireTime}. One table serves every
 * labukDB table, partitioned by {@code tablePath}.
 */
public final class DynamoDbLockProvider implements LockProvider {

    private static final String ATTR_TABLE_PATH = "tablePath";
    private static final String ATTR_FILE_NAME = "fileName";
    private static final String ATTR_TEMP_PATH = "tempPath";
    private static final String ATTR_COMPLETE = "complete";
    private static final String ATTR_EXPIRE_TIME = "expireTime";

    private final DynamoDbClient client;
    private final String tableName;

    public DynamoDbLockProvider(DynamoDbClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    /** Builds a client from {@code config} and ensures the backing table exists before returning. */
    public static DynamoDbLockProvider fromConfiguration(DynamoDbLockConfiguration config) {
        DynamoDbLockProvider provider = new DynamoDbLockProvider(config.buildClient(), config.tableName());
        provider.ensureTableExists();
        return provider;
    }

    /**
     * Mirrors Delta's own {@code tryEnsureTableExists}: create-if-missing,
     * on-demand billing (no RCU/WCU tuning burden for this project's v1),
     * poll until {@code ACTIVE}. TTL enablement is best-effort — DynamoDB
     * Local accepts {@code updateTimeToLive} but does not actually enforce
     * expiry, a known test-environment limitation, not a production risk.
     */
    void ensureTableExists() {
        try {
            TableDescription description = client.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build()).table();
            if (description.tableStatus() == TableStatus.ACTIVE) {
                return;
            }
        } catch (ResourceNotFoundException notYetCreated) {
            client.createTable(r -> r.tableName(tableName)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .keySchema(
                            KeySchemaElement.builder().attributeName(ATTR_TABLE_PATH).keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName(ATTR_FILE_NAME).keyType(KeyType.RANGE).build())
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName(ATTR_TABLE_PATH).attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName(ATTR_FILE_NAME).attributeType(ScalarAttributeType.S).build()));
            try {
                client.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                        .tableName(tableName)
                        .timeToLiveSpecification(TimeToLiveSpecification.builder()
                                .enabled(true).attributeName(ATTR_EXPIRE_TIME).build())
                        .build());
            } catch (RuntimeException ttlNotSupported) {
                // Best-effort only -- some local/emulated backends accept but don't enforce TTL.
            }
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

    @Override
    public void putEntry(ExternalCommitEntry entry, boolean overwrite) throws IOException {
        Map<String, AttributeValue> item = new java.util.HashMap<>();
        item.put(ATTR_TABLE_PATH, AttributeValue.builder().s(entry.tablePath()).build());
        item.put(ATTR_FILE_NAME, AttributeValue.builder().s(entry.fileName()).build());
        item.put(ATTR_TEMP_PATH, AttributeValue.builder().s(entry.tempPath()).build());
        item.put(ATTR_COMPLETE, AttributeValue.builder().s(String.valueOf(entry.complete())).build());
        if (entry.expireTime() != null) {
            item.put(ATTR_EXPIRE_TIME, AttributeValue.builder().n(String.valueOf(entry.expireTime())).build());
        }

        PutItemRequest.Builder request = PutItemRequest.builder().tableName(tableName).item(item);
        if (!overwrite) {
            request.conditionExpression("attribute_not_exists(" + ATTR_FILE_NAME + ")");
        }

        try {
            client.putItem(request.build());
        } catch (ConditionalCheckFailedException conflict) {
            throw new FileAlreadyExistsException(entry.tablePath() + "/" + entry.fileName());
        }
    }

    @Override
    public Optional<ExternalCommitEntry> getEntry(String tablePath, String fileName) throws IOException {
        GetItemResponse response = client.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .consistentRead(true) // GetItem defaults to eventually-consistent reads, which would
                                       // undermine the N-1 recovery check's correctness.
                .key(Map.of(
                        ATTR_TABLE_PATH, AttributeValue.builder().s(tablePath).build(),
                        ATTR_FILE_NAME, AttributeValue.builder().s(fileName).build()))
                .build());

        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        Map<String, AttributeValue> item = response.item();
        Long expireTime = item.containsKey(ATTR_EXPIRE_TIME) ? Long.valueOf(item.get(ATTR_EXPIRE_TIME).n()) : null;
        return Optional.of(new ExternalCommitEntry(
                item.get(ATTR_TABLE_PATH).s(),
                item.get(ATTR_FILE_NAME).s(),
                item.get(ATTR_TEMP_PATH).s(),
                Boolean.parseBoolean(item.get(ATTR_COMPLETE).s()),
                expireTime));
    }
}
