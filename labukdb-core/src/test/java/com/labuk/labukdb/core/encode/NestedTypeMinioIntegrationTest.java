package com.labuk.labukdb.core.encode;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves the recursive {@code STRUCT}/{@code MAP} type support (added this
 * session for {@code ArrowToParquetWriter}/{@code NestedTypeDecoder}) is
 * genuinely remote-capable, not just proven against local temp files —
 * same opt-in live-MinIO convention every other test in this project uses.
 *
 * <b>Deliberately not re-proving list-of-list or struct-containing-list
 * here</b> — MinIO's one known real gap is commit-log atomicity (see
 * {@code CommitWriterConcurrentMinioIntegrationTest}), not raw byte I/O,
 * which is already proven remote-capable for every Arrow type via {@code
 * RemoteWriteMinioIntegrationTest}/{@code RemoteReadMinioIntegrationTest}.
 * One {@code StructVector} case and one {@code MapVector} case cover the
 * two genuinely different nested vector shapes; re-running every local
 * nesting permutation remotely would only re-test S3A's generic transfer,
 * not anything nesting-specific.
 */
class NestedTypeMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO nested-type test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO nested-type test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO nested-type test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-nested-type-minio-test/" + UUID.randomUUID();
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

    private void writeVarCharNamed(NullableStructWriter writer, String fieldName, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar(fieldName).writeVarChar(0, bytes.length, buf);
        }
    }

    private void writeVarCharNoName(org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter writer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar().writeVarChar(0, bytes.length, buf);
        }
    }

    @Test
    void structColumn_roundTripsOverRealMinio() throws Exception {
        Field nameField = new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field personField = new Field("person", FieldType.nullable(ArrowType.Struct.INSTANCE),
                List.of(nameField, ageField));
        String dataFileUri = testRootUri + "/person.parquet";

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(personField)), allocator)) {
            batch.allocateNew();
            StructVector person = (StructVector) batch.getVector("person");
            NullableStructWriter writer = person.getWriter();

            writer.setPosition(0);
            writer.start();
            writeVarCharNamed(writer, "name", "Ada");
            writer.integer("age").writeInt(30);
            writer.end();
            writer.setValueCount(1);
            batch.setRowCount(1);

            new ArrowToParquetWriter().write(TableLocation.forRemote(dataFileUri, conf), batch);
        }

        try (VectorSchemaRoot result = new ParquetToArrowReader().read(
                TableLocation.forRemote(dataFileUri, conf), allocator, null, null, Map.of())) {
            assertEquals(1, result.getRowCount());
            StructVector resultPerson = (StructVector) result.getVector("person");
            Map<?, ?> row0 = (Map<?, ?>) resultPerson.getObject(0);
            assertEquals("Ada", row0.get("name").toString());
            assertEquals(30, ((Number) row0.get("age")).intValue());
        }
    }

    @Test
    void mapColumn_roundTripsOverRealMinio() throws Exception {
        Field keyField = new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field valueField = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field entriesField = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                List.of(keyField, valueField));
        Field mapField = new Field("attrs", FieldType.nullable(new ArrowType.Map(false)), List.of(entriesField));
        String dataFileUri = testRootUri + "/attrs.parquet";

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(mapField)), allocator)) {
            batch.allocateNew();
            MapVector attrs = (MapVector) batch.getVector("attrs");
            UnionMapWriter writer = attrs.getWriter();

            writer.setPosition(0);
            writer.startMap();
            writer.startEntry();
            writeVarCharNoName(writer.key(), "height");
            writer.value().integer().writeInt(180);
            writer.endEntry();
            writer.startEntry();
            writeVarCharNoName(writer.key(), "weight");
            writer.value().integer().writeInt(75);
            writer.endEntry();
            writer.endMap();
            writer.setValueCount(1);
            batch.setRowCount(1);

            new ArrowToParquetWriter().write(TableLocation.forRemote(dataFileUri, conf), batch);
        }

        try (VectorSchemaRoot result = new ParquetToArrowReader().read(
                TableLocation.forRemote(dataFileUri, conf), allocator, null, null, Map.of())) {
            assertEquals(1, result.getRowCount());
            MapVector resultAttrs = (MapVector) result.getVector("attrs");
            List<?> entries = (List<?>) resultAttrs.getObject(0);
            assertEquals(2, entries.size());
            Map<?, ?> firstEntry = (Map<?, ?>) entries.get(0);
            assertEquals("height", firstEntry.get("key").toString());
            assertEquals(180, ((Number) firstEntry.get("value")).intValue());
        }
    }
}
