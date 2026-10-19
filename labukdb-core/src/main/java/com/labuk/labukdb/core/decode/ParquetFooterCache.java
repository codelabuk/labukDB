package com.labuk.labukdb.core.decode;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches a file's Parquet schema, keyed by absolute path — so
 * {@code GetFlightInfo}'s "what does this look like" schema inference,
 * which runs on every single query even when nothing about the table
 * changed, doesn't reopen and re-parse the file's footer every time.
 *
 * <b>Safe to cache indefinitely</b> — same reasoning as everywhere else in
 * this project: once a Delta commit adds a file, that file's bytes never
 * change. A file only ever gets logically removed via a later commit,
 * never mutated in place. A cache entry could only go stale if the exact
 * same path were reused for genuinely different content, which a
 * correctly operating Delta writer never does.
 *
 * <b>Scope, stated plainly:</b> this only accelerates schema-only access.
 * The full row-data read path in {@link ParquetToArrowReader#read} still
 * opens and parses the footer itself when it actually scans a file —
 * avoiding that too would need a {@code ParquetFileReader} constructor
 * that accepts a pre-read footer, which is a real further optimization,
 * but whose exact availability in this project's pinned parquet-mr version
 * hasn't been verified here. Left as a named follow-up, not guessed at.
 */
final class ParquetFooterCache {

    private final Map<String, MessageType> schemas = new ConcurrentHashMap<>();

    MessageType schemaOf(File parquetFile) throws IOException {
        String key = parquetFile.getAbsolutePath();
        MessageType cached = schemas.get(key);
        if (cached != null) {
            return cached;
        }
        MessageType schema = readSchemaFromDisk(parquetFile);
        schemas.put(key, schema);
        return schema;
    }

    int size() {
        return schemas.size();
    }

    private MessageType readSchemaFromDisk(File parquetFile) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            return fileReader.getFooter().getFileMetaData().getSchema();
        }
    }
}
