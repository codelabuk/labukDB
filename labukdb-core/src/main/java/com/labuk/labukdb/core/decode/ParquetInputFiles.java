package com.labuk.labukdb.core.decode;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Builds a Parquet-mr {@link InputFile} for either local or remote
 * (S3/MinIO/ADLS) storage — the decode-layer equivalent of what
 * {@code LogStorage} does for the log layer.
 *
 * <b>Additive, not a replacement:</b> every existing {@code File}-based
 * method on {@link ParquetToArrowReader}/{@link ParquetFooterCache} is
 * unchanged and still works exactly as before — internally, they now
 * build a local {@code InputFile} via {@link #local} and delegate to a
 * new {@code InputFile}-based overload. That overload is what a caller
 * wanting remote data would use directly via {@link #remote}.
 *
 * <b>Integration gap, stated plainly:</b> nothing in {@code DeltaFlightProducer},
 * the catalog, or the CLI automatically chooses {@link #remote} based on a
 * URI scheme yet — they all still resolve local {@code File}s exclusively.
 * This class makes the decode engine itself remote-capable; wiring the
 * Flight server to actually dispatch to it based on a table's location is
 * the next integration step, not done here.
 */
public final class ParquetInputFiles {

    private ParquetInputFiles() {
    }

    public static InputFile local(File file) {
        try {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
            return HadoopInputFile.fromPath(hadoopPath, new Configuration());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build InputFile for " + file, e);
        }
    }

    /** For S3/MinIO/ADLS — pass a Configuration with the connector's endpoint/credentials already set. */
    public static InputFile remote(org.apache.hadoop.fs.Path path, Configuration conf) {
        try {
            return HadoopInputFile.fromPath(path, conf);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build InputFile for " + path, e);
        }
    }
}
