package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Settles, with real evidence rather than a documented expectation, whether
 * {@link IcebergTableWriter} (path-based {@code HadoopTables}) survives
 * genuinely concurrent writers against real MinIO. Iceberg's own
 * documentation calls {@code HadoopCatalog}/{@code HadoopTables} unsafe for
 * concurrent use on S3-compatible storage — its commit is a rename, and
 * S3-compatible object storage's rename is a non-atomic copy+delete, not a
 * real atomic metadata swap the way local-disk rename is. Until this test,
 * that was "expected broken, not tested" in this project's own status
 * tracking — this makes it a confirmed, evidenced fact either way.
 *
 * <b>Genuinely separate JVM processes, not threads — found to matter, not
 * assumed.</b> A first attempt using threads within one JVM passed cleanly
 * (8/8), which turned out to be a false negative: bytecode inspection of
 * {@code iceberg-core} confirmed {@code HadoopTables}' default {@code
 * LockManager} is {@code LockManagers.LOCK_MANAGER_DEFAULT} — a static,
 * per-JVM, plain-{@code HashMap}-backed {@code InMemoryLockManager}. Threads
 * in one process share that static singleton and get perfectly serialized
 * by it, never exercising the real {@code FileSystem.rename()} risk at all.
 * Separate processes each get their own independent in-memory lock manager
 * (the singleton is per-classloader, not cross-process), which is the only
 * way to actually reach the storage layer's real behavior — see {@link
 * IcebergConcurrentWriterWorker}, launched here via {@link ProcessBuilder}.
 *
 * Opt-in, same {@code MINIO_*} env-var gate as every other MinIO
 * integration test in this project.
 */
class IcebergTableWriterConcurrentMinioIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()));

    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Iceberg concurrency test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Iceberg concurrency test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Iceberg concurrency test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-iceberg-concurrent-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    void appendBatch_concurrentSeparateProcessesAgainstRealMinio_allCommitsLandOrFailCleanly(@TempDir java.nio.file.Path outputDir) throws Exception {
        new HadoopTables(conf).create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), testRootUri);

        int writerCount = 8;
        String javaBin = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        List<Process> processes = new ArrayList<>();
        List<java.io.File> outputFiles = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            java.io.File outputFile = outputDir.resolve("writer-" + i + ".out").toFile();
            outputFiles.add(outputFile);
            // ProcessBuilder doesn't inherit the parent JVM's own command-line
            // flags (only its environment) -- this project's root pom sets
            // --add-opens for Arrow's off-heap memory access via surefire's
            // argLine, which a plain child `java` invocation won't have
            // unless passed explicitly. Found the hard way: every worker
            // crashed immediately on Arrow's MemoryUtil init without it.
            ProcessBuilder pb = new ProcessBuilder(javaBin,
                    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
                    "-cp", classpath,
                    IcebergConcurrentWriterWorker.class.getName(), testRootUri, String.valueOf(i));
            // Redirect straight to a file rather than relying on getInputStream()'s
            // pipe -- found the hard way: waitFor()-then-drain deadlocks for real.
            // A child JVM's routine SLF4J/log4j/Hadoop startup chatter on stdout
            // can fill the OS pipe buffer before it ever prints its one real
            // output line; the child then blocks on write() while this process
            // blocks on waitFor(), and neither side ever makes progress again.
            // Redirecting to a file sidesteps the pipe entirely.
            pb.redirectOutput(outputFile);
            pb.redirectErrorStream(true);
            // Inherits this JVM's environment by default -- MINIO_* reaches
            // the child via normal process env inheritance, never written
            // to a file or passed as a visible command-line argument.
            processes.add(pb.start());
        }

        List<String> outcomes = new ArrayList<>();
        for (int i = 0; i < processes.size(); i++) {
            Process process = processes.get(i);
            boolean finished = process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                outcomes.add("TIMED_OUT:writer-" + i);
                continue;
            }
            List<String> lines = java.nio.file.Files.readAllLines(outputFiles.get(i).toPath(), StandardCharsets.UTF_8);
            String resultLine = lines.stream().filter(l -> l.startsWith("SUCCESS") || l.startsWith("FAILED")).findFirst()
                    .orElse("NO_RESULT_LINE:writer-" + i + " (exit=" + process.exitValue() + ", output=" + lines + ")");
            outcomes.add(resultLine);
        }

        long successCount = outcomes.stream().filter(o -> o.startsWith("SUCCESS")).count();
        System.out.println("=== ICEBERG CONCURRENT MINIO WRITE OUTCOMES ===");
        outcomes.forEach(System.out::println);
        System.out.println("successCount=" + successCount + " / " + writerCount);

        // The real question this test answers: does the number of *visible,
        // readable* committed files match the number of writers that
        // genuinely reported success? If rename-based commit silently loses
        // writes on this storage (the documented risk), successCount would
        // overcount what's actually visible -- so this checks the real
        // snapshot, not just whether appendBatch() threw.
        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();
        long visibleFiles = snapshot.getFiles().size();
        System.out.println("visibleFilesInSnapshot=" + visibleFiles);

        Set<String> expectedServices = new HashSet<>();
        Set<String> actualServices = new HashSet<>();
        for (String outcome : outcomes) {
            if (outcome.startsWith("SUCCESS")) {
                expectedServices.add(outcome.substring("SUCCESS:".length()));
            }
        }
        for (DataFileRef file : snapshot.getFiles()) {
            TableLocation dataFile = TableLocation.forRemote(file.getPath(), conf);
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("service"), null, Map.of())) {
                actualServices.add(new String(
                        ((VarCharVector) readBack.getVector("service")).get(0), StandardCharsets.UTF_8));
            }
        }

        assertEquals(expectedServices, actualServices,
                "every writer that reported SUCCESS must have a genuinely visible, readable file -- "
                        + "a mismatch here is the real-world consequence of Iceberg's documented "
                        + "HadoopTables-on-S3 unsafety: a writer believing it committed when the "
                        + "commit was actually lost to a racing rename");
    }
}
