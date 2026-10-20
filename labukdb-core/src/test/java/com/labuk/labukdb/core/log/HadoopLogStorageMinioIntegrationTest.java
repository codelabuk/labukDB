package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.MinioConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves {@code createCommitFile}'s atomicity contract against a REAL
 * MinIO cluster, not a local-filesystem substitution — the actual
 * unverified claim {@link HadoopLogStorage}'s javadoc used to carry until
 * this was run against {@code quay.io/minio/minio:latest} on 2026-10-20.
 *
 * <b>This is a live-infrastructure integration test, opt-in by design</b> —
 * it needs real network access to a real MinIO deployment, which a normal
 * CI run or another developer's machine won't have. It reads connection
 * details from environment variables only (never hardcoded, never
 * written to any file) and skips itself — reported SKIPPED, not FAILED —
 * if they're not set, via JUnit 5's {@link org.junit.jupiter.api.Assumptions}.
 * {@code mvn test} stays green everywhere; this test only actually runs,
 * and only actually proves anything, on a machine configured to reach a
 * real cluster.
 *
 * <b>To run it for real</b>, set before {@code mvn test}:
 * <pre>
 *   MINIO_ENDPOINT=https://your-minio-host
 *   MINIO_ACCESS_KEY=...
 *   MINIO_SECRET_KEY=...
 *   MINIO_TEST_BUCKET=warehouse   (optional — defaults to "warehouse";
 *                                  must already exist, this test does not
 *                                  create buckets)
 * </pre>
 * Every object this test creates lives under a fresh random path inside
 * that bucket and is deleted in {@code @AfterEach}, regardless of outcome.
 */
class HadoopLogStorageMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testDir;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testDir = new Path("s3a://" + bucket + "/labukdb-minio-integration-test/" + UUID.randomUUID());
        fileSystem = testDir.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (fileSystem != null && testDir != null) {
            fileSystem.delete(testDir, true);
        }
    }

    @Test
    void createCommitFile_pattern_isGenuinelyAtomicAgainstRealMinio() throws Exception {
        Path commitFile = new Path(testDir, "_delta_log/00000000000000000000.json");
        String content = "{\"commitInfo\":{\"operation\":\"labukdb MinIO integration test\"}}\n";

        // Step 1: atomic create — exactly HadoopLogStorage.createCommitFile's own implementation.
        try (FSDataOutputStream out = fileSystem.create(commitFile, false)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        // Step 2: read back.
        assertEquals(content.trim(), readLine(commitFile));

        // Step 3: a conflicting second create must be rejected, not silently overwrite.
        IOException thrown = assertThrows(IOException.class, () -> {
            try (FSDataOutputStream out2 = fileSystem.create(commitFile, false)) {
                out2.write("{\"a different, conflicting payload\":{}}\n".getBytes(StandardCharsets.UTF_8));
            }
        });
        // FileAlreadyExistsException specifically is what Hadoop's S3A
        // throws when MinIO honors the conditional-write header, as
        // verified here; documented loosely as IOException above in case
        // a different MinIO build surfaces a different IOException
        // subtype while still correctly rejecting the overwrite.
        assertInstanceOf(FileAlreadyExistsException.class, thrown,
                "expected the specific FileAlreadyExistsException this MinIO build is known to throw");

        // Step 4: the original writer's content must have survived untouched.
        assertEquals(content.trim(), readLine(commitFile));
    }

    private String readLine(Path path) throws Exception {
        try (FSDataInputStream in = fileSystem.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.readLine();
        }
    }
}
