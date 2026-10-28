package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Standalone single-shot worker, launched as a genuinely separate JVM
 * process (not a thread) by {@link IcebergTableWriterConcurrentMinioIntegrationTest}
 * — the only way to exercise the real storage-layer commit risk, since
 * {@code HadoopTables}' default {@code LockManager} is a static, per-JVM,
 * in-memory {@code HashMap} (confirmed via bytecode inspection of {@code
 * LockManagers.LOCK_MANAGER_DEFAULT}): N threads inside one JVM would be
 * silently serialized by that lock and never exercise the underlying
 * {@code FileSystem.rename()} primitive's real concurrent behavior at all.
 *
 * Args: {@code <tableUri> <writerId>}. Reads {@code MINIO_ENDPOINT}/
 * {@code MINIO_ACCESS_KEY}/{@code MINIO_SECRET_KEY} from its environment
 * (inherited from the parent process, never passed as command-line
 * arguments, so nothing credential-shaped appears in a process listing).
 * Prints exactly one line: {@code SUCCESS:<writerId>} or
 * {@code FAILED:<writerId>:<exceptionClass>:<message>}.
 */
public final class IcebergConcurrentWriterWorker {

    public static void main(String[] args) throws Exception {
        String tableUri = args[0];
        String writerId = args[1];

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);

        try {
            IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forRemote(tableUri, conf));
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                 VectorSchemaRoot batch = buildBatch(allocator, "writer-" + writerId, Long.parseLong(writerId))) {
                writer.appendBatch(batch, Map.of());
            }
            System.out.println("SUCCESS:writer-" + writerId);
        } catch (Exception e) {
            System.out.println("FAILED:writer-" + writerId + ":" + e.getClass().getSimpleName() + ":" + e.getMessage());
        }
    }

    private static VectorSchemaRoot buildBatch(BufferAllocator allocator, String service, long amount) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        ((BigIntVector) root.getVector("amount")).setSafe(0, amount);
        root.setRowCount(1);
        return root;
    }
}
