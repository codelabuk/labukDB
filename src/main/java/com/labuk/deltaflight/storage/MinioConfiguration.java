package com.labuk.deltaflight.storage;

import org.apache.hadoop.conf.Configuration;

/**
 * Builds the Hadoop {@link Configuration} an {@code s3a://} location needs.
 *
 * <b>The MinIO-specific parts, which are easy to get wrong:</b>
 * <ul>
 *   <li>{@code fs.s3a.endpoint} — MinIO isn't AWS, so the endpoint must
 *       point at your MinIO server (e.g. {@code http://localhost:9000}).</li>
 *   <li>{@code fs.s3a.path.style.access=true} — <b>required for MinIO.</b>
 *       AWS uses virtual-host-style addressing
 *       ({@code bucket.s3.amazonaws.com}); MinIO serves buckets as path
 *       segments ({@code localhost:9000/bucket}). Without this, requests
 *       go to a hostname that doesn't resolve.</li>
 *   <li>{@code fs.s3a.connection.ssl.enabled} — usually false for a local
 *       MinIO over plain HTTP; leaving it true against an http:// endpoint
 *       fails in a confusing way.</li>
 *   <li>{@code fs.s3a.aws.credentials.provider} set to
 *       {@code SimpleAWSCredentialsProvider} — forces the explicitly
 *       supplied key/secret to be used rather than Hadoop hunting for
 *       ambient AWS credentials that don't exist for MinIO.</li>
 * </ul>
 *
 * <b>Requires {@code hadoop-aws} on the classpath</b> — declared as an
 * optional dependency in this project's pom, so it IS present in this
 * project's own build, but a downstream consumer depending on this library
 * would need to add it explicitly.
 *
 * <b>Not verified against a real MinIO instance</b> — no way to run one in
 * the environment this was written in. These are the standard, documented
 * settings, but a real first run is the actual test.
 */
public final class MinioConfiguration {

    private MinioConfiguration() {
    }

    /**
     * @param endpoint  e.g. {@code http://localhost:9000}
     * @param accessKey MinIO access key
     * @param secretKey MinIO secret key
     */
    public static Configuration forMinio(String endpoint, String accessKey, String secretKey) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.endpoint", endpoint);
        conf.set("fs.s3a.access.key", accessKey);
        conf.set("fs.s3a.secret.key", secretKey);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", String.valueOf(endpoint.startsWith("https://")));
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return conf;
    }

    /**
     * Reads the same settings from environment variables — the shape that
     * actually fits running the Flight server as a service (Docker, k8s)
     * where credentials come from the environment, not code.
     *
     * Reads: {@code MINIO_ENDPOINT}, {@code MINIO_ACCESS_KEY},
     * {@code MINIO_SECRET_KEY}. Returns a plain default Configuration if
     * the endpoint isn't set, so a local-only deployment doesn't need any
     * of these defined.
     */
    public static Configuration fromEnvironment() {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return new Configuration();
        }
        return forMinio(endpoint,
                System.getenv("MINIO_ACCESS_KEY"),
                System.getenv("MINIO_SECRET_KEY"));
    }
}
