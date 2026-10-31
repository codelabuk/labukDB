package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
 * The Iceberg counterpart of {@code core.log.TableMergerConcurrentMinioIntegrationTest}
 * (which found 7 of 8 concurrent Delta merge-inserts silently lost) —
 * settles, with real evidence, whether {@link IcebergTableMerger}'s own
 * {@code Table.newOverwrite()} commit is exposed to the same confirmed-
 * broken no-lock race on real MinIO, going through the identical
 * {@code HadoopTableOperations.renameToFinal} rename-based commit path
 * {@code IcebergTableWriter.appendBatch} already proved broken.
 *
 * Genuinely separate JVM processes, not threads — the same reason
 * {@code IcebergTableWriterConcurrentMinioIntegrationTest} needs them:
 * {@code HadoopTables}' default {@code LockManager} is a static,
 * per-JVM, in-memory {@code HashMap}; threads in one process would be
 * falsely serialized by it and never exercise the real race. See
 * {@link IcebergConcurrentMergeWorker}.
 *
 * Opt-in, same {@code MINIO_*} env-var gate as every other MinIO
 * integration test in this project.
 */
class IcebergTableMergerConcurrentMinioIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "service", Types.StringType.get()));

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Iceberg merge-concurrency test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Iceberg merge-concurrency test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Iceberg merge-concurrency test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-iceberg-merge-concurrent-test/" + UUID.randomUUID();
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
    void merge_concurrentSeparateProcessesAgainstRealMinio_noLock_revealsActualBehavior(
            @TempDir java.nio.file.Path outputDir) throws Exception {
        new HadoopTables(conf).create(ICEBERG_SCHEMA, UNPARTITIONED, SortOrder.unsorted(), Map.of(), testRootUri);

        int writerCount = 8;
        String javaBin = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        List<Process> processes = new ArrayList<>();
        List<java.io.File> outputFiles = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            java.io.File outputFile = outputDir.resolve("writer-" + i + ".out").toFile();
            outputFiles.add(outputFile);
            ProcessBuilder pb = new ProcessBuilder(javaBin,
                    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
                    "-cp", classpath,
                    IcebergConcurrentMergeWorker.class.getName(), testRootUri, String.valueOf(i));
            pb.redirectOutput(outputFile);
            pb.redirectErrorStream(true);
            processes.add(pb.start());
        }

        List<String> outcomes = new ArrayList<>();
        for (int i = 0; i < processes.size(); i++) {
            Process process = processes.get(i);
            boolean finished = process.waitFor(120, java.util.concurrent.TimeUnit.SECONDS);
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
        System.out.println("=== ICEBERG TABLEMERGER NO-LOCK MINIO RESULT ===");
        outcomes.forEach(System.out::println);
        System.out.println("successCount=" + successCount + " / " + writerCount);

        NormalizedSnapshot finalSnapshot = new IcebergTableFormat(testRootUri, conf).latest();
        Set<Long> actualIds = new HashSet<>();
        for (DataFileRef file : finalSnapshot.getFiles()) {
            TableLocation dataFile = TableLocation.forRemote(file.getPath(), conf);
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("id"), null, Map.of())) {
                for (int row = 0; row < readBack.getRowCount(); row++) {
                    actualIds.add(((BigIntVector) readBack.getVector("id")).get(row));
                }
            }
        }

        Set<Long> expectedIds = new HashSet<>();
        for (int i = 0; i < writerCount; i++) {
            expectedIds.add((long) i);
        }

        System.out.println("expectedIds=" + expectedIds);
        System.out.println("actualIds=" + actualIds);

        // Intentionally the real discovery pass, not an assumed outcome --
        // mirrors TableMergerConcurrentMinioIntegrationTest's identical
        // framing for Delta.
        assertEquals(expectedIds, actualIds,
                "every writer's insert must be genuinely present -- a mismatch here is the real "
                        + "consequence of IcebergTableMerger's no-lock OverwriteFiles commit on real MinIO");
    }
}
