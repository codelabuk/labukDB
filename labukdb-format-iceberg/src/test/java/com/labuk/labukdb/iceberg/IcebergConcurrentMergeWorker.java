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
 * process — the same reasoning {@link IcebergConcurrentWriterWorker}'s
 * own javadoc gives applies identically here: {@code HadoopTables}'
 * default {@code LockManager} is a static, per-JVM, in-memory {@code
 * HashMap}, so N threads inside one JVM would be silently serialized by
 * it and never exercise {@code IcebergTableMerger}'s real remote commit
 * risk at all. Each worker performs one insert-only {@code merge()} call
 * with a distinct key, into the same (initially empty) partition.
 *
 * Args: {@code <tableUri> <writerId>}. Reads {@code MINIO_ENDPOINT}/
 * {@code MINIO_ACCESS_KEY}/{@code MINIO_SECRET_KEY} from its environment.
 * Prints exactly one line: {@code SUCCESS:<writerId>} or
 * {@code FAILED:<writerId>:<exceptionClass>:<message>}.
 */
public final class IcebergConcurrentMergeWorker {

    public static void main(String[] args) throws Exception {
        String tableUri = args[0];
        String writerId = args[1];

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);

        try {
            TableLocation location = TableLocation.forRemote(tableUri, conf);
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                 VectorSchemaRoot inserts = buildBatch(allocator, Long.parseLong(writerId), "writer-" + writerId)) {
                new IcebergTableMerger().merge(location, Map.of(), List.of("id"), inserts, null, null, allocator);
            }
            System.out.println("SUCCESS:writer-" + writerId);
        } catch (Exception e) {
            System.out.println("FAILED:writer-" + writerId + ":" + e.getClass().getSimpleName() + ":" + e.getMessage());
        }
    }

    private static VectorSchemaRoot buildBatch(BufferAllocator allocator, long id, String service) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(idField, serviceField)), allocator);
        root.allocateNew();
        ((BigIntVector) root.getVector("id")).setSafe(0, id);
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
        return root;
    }
}
