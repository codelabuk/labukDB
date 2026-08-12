# Hadoop Usage in delta-flight-engine

## Which Component

Hadoop **Common** (`hadoop-client`) — specifically its filesystem abstraction
layer (`org.apache.hadoop.fs.FileSystem` / `Path` / `Configuration`), plus the
optional cloud connectors `hadoop-aws` (S3/MinIO via `s3a://`) and
`hadoop-azure` (ADLS Gen2 via `abfss://`).

There is **no** MapReduce, YARN, or HDFS-daemon usage — it's purely the
`FileSystem` API used as a pluggable storage abstraction so the same code can
read local disk, S3, or ADLS depending on the URI scheme. Parquet's
`parquet-hadoop` module also rides on this (`HadoopInputFile` wraps a Hadoop
`FileSystem` to give Parquet random-access reads).

## Maven Dependencies (`pom.xml`)

| Artifact | Version | Purpose                                                                  |
|---|---|--------------------------------------------------------------------------|
| `org.apache.hadoop:hadoop-client` | 3.3.6 | Core `FileSystem`/`Configuration` plumbing; required by `parquet-hadoop` |
| `org.apache.hadoop:hadoop-aws` | 3.3.6 (optional) | Enables `s3a://` for S3 and MinIO                                        |
| `org.apache.hadoop:hadoop-azure` | 3.3.6 (optional) | Enables `abfss:g` for Azure Data Lake Storage Gen2                       |

## Files Using Hadoop APIs Directly (`src/main`)

| File | What it uses |
|---|---|
| `log/HadoopLogStorage.java` | Core integration — `FileSystem`, `Path`, `FSDataInputStream`, `FileStatus` to list/read/stat Delta log files (`listStatus`, `open`, `getFileStatus`) |
| `log/SnapshotManager.java` | Takes a Hadoop `Path` + `Configuration`, constructs `HadoopLogStorage` |
| `decode/ParquetInputFiles.java` | `HadoopInputFile.fromPath(...)` — wraps local files as a Parquet `InputFile` |
| `decode/ParquetFooterCache.java` | Same — `HadoopInputFile` + `ParquetFileReader` for footer reads |
| `storage/TableLocation.java` | Holds a Hadoop `Path`/`Configuration` per table, exposes `hadoopPath()` / `hadoopConf()` |
| `storage/MinioConfiguration.java` | Builds a `Configuration` with `fs.s3a.*` keys (endpoint, keys, path-style access) for MinIO/S3 |
| `storage/AdlsConfiguration.java` | Builds a `Configuration` with `fs.azure.*` keys (shared-key or service-principal auth) for ADLS |
| `cli/TableResolver.java` | Assembles a merged `Configuration` from `AdlsConfiguration` / `MinioConfiguration` env vars |
| `flight/DeltaFlightProducer.java` | Holds the merged storage `Configuration`, passes it down to `SnapshotManager` / `TableLocation` per request |

`log/LocalNioLogStorage.java` is the non-Hadoop counterpart (plain
`java.nio.file`) — `HadoopLogStorage` is an alternate implementation of the
same `LogStorage` interface, not a replacement.

## Related Test Files

- `flight/FlightIntegrationTest.java`
- `decode/ProjectionPredicateBugTest.java`
- `storage/TableLocationTest.java`
- `decode/ParquetToArrowReaderTest.java`
- `log/HadoopBackedSnapshotManagerTest.java`
- `log/CheckpointReaderTest.java`
- `decode/ParquetFooterCacheTest.java`

These mainly exercise `HadoopLogStorage` / `HadoopInputFile` against local
paths. Per a comment in `HadoopLogStorage.java`, real S3/ADLS endpoints are
**not** actually exercised in this environment — only Hadoop's local
filesystem implementation is tested, on the assumption that it behaves
equivalently to the cloud connectors for the parts that matter.
