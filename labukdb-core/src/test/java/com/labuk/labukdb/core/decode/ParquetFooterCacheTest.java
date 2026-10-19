package com.labuk.labukdb.core.decode;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ParquetFooterCacheTest {

    private static final String SCHEMA_STRING = "message test { required int64 amount; }";

    private File writeMinimalFile(Path dir, String name) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STRING);
        File file = dir.resolve(name).toFile();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withConf(conf).withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("amount", 1L));
        }
        return file;
    }

    @Test
    void repeatedLookups_sameFile_onlyOneCacheEntry(@TempDir Path tempDir) throws Exception {
        File file = writeMinimalFile(tempDir, "a.parquet");
        ParquetFooterCache cache = new ParquetFooterCache();

        MessageType first = cache.schemaOf(file);
        MessageType second = cache.schemaOf(file);

        assertEquals(1, cache.size(), "a second lookup of the same file must be a cache hit, not a new entry");
        assertEquals(first.getFields().size(), second.getFields().size());
        assertEquals("amount", first.getFields().get(0).getName());
    }

    @Test
    void differentFiles_getSeparateEntries(@TempDir Path tempDir) throws Exception {
        File fileA = writeMinimalFile(tempDir, "a.parquet");
        File fileB = writeMinimalFile(tempDir, "b.parquet");
        ParquetFooterCache cache = new ParquetFooterCache();

        cache.schemaOf(fileA);
        cache.schemaOf(fileB);

        assertEquals(2, cache.size());
    }
}
