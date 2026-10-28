package com.labuk.labukdb.core.storage;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.Optional;

/**
 * Builds the {@link DynamoDbClient} a {@code DynamoDbLockProvider} needs —
 * the external lock backing {@code CommitWriter}'s opt-in multi-writer OCC
 * fix (see {@code LockProvider}'s javadoc for why this exists).
 *
 * <b>Zero new Maven dependency</b> — {@code DynamoDbClient} is already on
 * this project's classpath transitively, via {@code hadoop-aws}'s bundled
 * {@code software.amazon.awssdk:bundle} (confirmed by compiling against it
 * directly, not assumed).
 *
 * <b>Works identically against real AWS DynamoDB or a self-hosted
 * {@code amazon/dynamodb-local} container</b> — the same official, Docker-
 * runnable, no-AWS-account-needed emulator this project's own tests use, so
 * a MinIO-only self-hosted deployment doesn't need an AWS account to get a
 * real external lock. Verified empirically: a live 8-thread conditional-
 * {@code PutItem} race against {@code amazon/dynamodb-local} produced
 * exactly 1 success and 7 clean {@code ConditionalCheckFailedException}s —
 * the real atomicity guarantee MinIO's own S3 conditional-create lacks.
 */
public final class DynamoDbLockConfiguration {

    private final String tableName;
    private final Region region;
    private final String endpointOverride;
    private final String accessKey;
    private final String secretKey;
    private final long expirySeconds;

    private DynamoDbLockConfiguration(String tableName, Region region, String endpointOverride,
                                       String accessKey, String secretKey, long expirySeconds) {
        this.tableName = tableName;
        this.region = region;
        this.endpointOverride = endpointOverride;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.expirySeconds = expirySeconds;
    }

    public String tableName() {
        return tableName;
    }

    public long expirySeconds() {
        return expirySeconds;
    }

    public DynamoDbClient buildClient() {
        DynamoDbClientBuilder builder = DynamoDbClient.builder().region(region);
        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
        }
        if (accessKey != null && !accessKey.isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        }
        return builder.build();
    }

    /**
     * Reads {@code DYNAMODB_LOCK_TABLE}, {@code DYNAMODB_REGION} (default
     * {@code us-east-1}, matching Delta's own
     * {@code S3DynamoDBLogStore.ddb.region} default), {@code DYNAMODB_ENDPOINT}
     * (optional — set for {@code amazon/dynamodb-local}, e.g.
     * {@code http://localhost:8000}; omit for real AWS), {@code
     * DYNAMODB_ACCESS_KEY}/{@code DYNAMODB_SECRET_KEY} (optional — needed
     * for DynamoDB Local's fake static credentials, omit for real AWS to
     * use Hadoop/AWS's own default credential chain), and {@code
     * DYNAMODB_LOCK_EXPIRY_SECONDS} (default one day, matching Delta's own
     * default entry-expiration delay).
     *
     * Empty if {@code DYNAMODB_LOCK_TABLE} isn't set — there's no sensible
     * default {@code LockProvider} the way there's a default empty Hadoop
     * {@code Configuration}, so this is {@code Optional} rather than
     * {@code MinioConfiguration}/{@code AdlsConfiguration}'s "return a
     * no-op object" shape: a deployment not opting into the lock doesn't
     * need any of these set.
     */
    public static Optional<DynamoDbLockConfiguration> fromEnvironment() {
        return fromEnvironment("DYNAMODB_LOCK_TABLE", "DYNAMODB_LOCK_EXPIRY_SECONDS", 86_400);
    }

    /**
     * Same connection env vars ({@code DYNAMODB_REGION}/{@code _ENDPOINT}/
     * {@code _ACCESS_KEY}/{@code _SECRET_KEY}) as {@link #fromEnvironment()},
     * but a caller-chosen table-name and expiry env var and default — so a
     * second, independent lock consumer (e.g. Iceberg's {@code
     * DynamoDbLockManager}) can reuse this same connection-building logic
     * against its own, differently-shaped backing table without colliding
     * with this class's original Delta-lock env var names.
     */
    public static Optional<DynamoDbLockConfiguration> fromEnvironment(
            String tableNameEnvVar, String expirySecondsEnvVar, long defaultExpirySeconds) {
        String tableName = System.getenv(tableNameEnvVar);
        if (tableName == null || tableName.isBlank()) {
            return Optional.empty();
        }
        String regionName = System.getenv("DYNAMODB_REGION");
        Region region = Region.of(regionName == null || regionName.isBlank() ? "us-east-1" : regionName);
        String expiryRaw = System.getenv(expirySecondsEnvVar);
        long expirySeconds = (expiryRaw == null || expiryRaw.isBlank()) ? defaultExpirySeconds : Long.parseLong(expiryRaw.trim());

        return Optional.of(new DynamoDbLockConfiguration(tableName, region,
                System.getenv("DYNAMODB_ENDPOINT"),
                System.getenv("DYNAMODB_ACCESS_KEY"),
                System.getenv("DYNAMODB_SECRET_KEY"),
                expirySeconds));
    }
}
