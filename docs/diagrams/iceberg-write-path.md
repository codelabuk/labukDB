# labukDB — Iceberg Write Path (`labukdb-format-iceberg`)

```mermaid
flowchart TB
    classDef icebergModule fill:#D9F2C4,stroke:#534AB7,color:#26215C
    classDef storageNode fill:#9FE1CB,stroke:#0F6E56,color:#04342C

    IT[("Iceberg table<br/>(metadata/ + Parquet)")]:::storageNode

    ITCR["IcebergTableCreator<br/>(path-based via HadoopTables.create,<br/>or catalog-based via Catalog.createTable;<br/>identity-only partitioning)"]:::icebergModule
    AIS["ArrowToIcebergSchema<br/>(Arrow Schema → org.apache.iceberg.Schema;<br/>Struct/Map get their own field IDs<br/>via one running counter)"]:::icebergModule
    IDFW["IcebergDataFileWriter<br/>(write a new Parquet file + build<br/>the matching DataFile — shared by<br/>appendBatch and IcebergTableMerger)"]:::icebergModule

    ITW["IcebergTableWriter<br/>(append-only; Table.newAppend()<br/>retries OCC conflicts internally —<br/>no hand-built retry loop needed)"]:::icebergModule
    ITMRG["IcebergTableMerger<br/>(upsert/MERGE by key; commits via<br/>Table.newOverwrite())"]:::icebergModule
    PRM["PartitionRowMerger<br/>(shared with Delta's TableMerger —<br/>same merge-by-key semantics,<br/>not a second independently-evolving copy)"]:::icebergModule
    ICOMP["IcebergCompactor<br/>(bin-pack small files; commits via<br/>Table.newRewrite() — the real<br/>non-data-changing commit type,<br/>confirmed via javap, not newOverwrite())"]:::icebergModule
    IVAC["IcebergVacuum<br/>(thin wrapper over Iceberg's own<br/>native Table.expireSnapshots())"]:::icebergModule

    IDDBL["DynamoDbLockManager<br/>(implements Iceberg's own<br/>LockManager extension point —<br/>lease-based acquire/release)"]:::icebergModule
    ILMC["IcebergLockManagerConfig<br/>(opt-in: sets Configuration's<br/>'lock-impl' property when<br/>ICEBERG_LOCK_DYNAMODB_TABLE is set)"]:::icebergModule
    RWSW["RemoteWriteSafetyWarning<br/>(shared with Delta's write path)"]:::icebergModule

    IRC["IcebergRestConfiguration<br/>(ICEBERG_REST_* env vars → RESTCatalog)"]:::icebergModule
    IRL["IcebergRestLocation<br/>(icebergrest://ns.table parsing)"]:::icebergModule

    ITCR -- "build Iceberg schema" --> AIS
    ITCR -- "create table (version 0)" --> IT
    ITCR -. "table immediately usable<br/>by existing, unmodified writer" .-> ITW

    ITW -- "commit via iceberg-core<br/>(Table.newAppend)" --> IT
    ITW -- "appendBatch() delegates<br/>write+build-DataFile step" --> IDFW
    ITW -. "construction —<br/>applyIfConfigured(conf)" .-> ILMC
    ITW -. "construction —<br/>warns if remote + no lock" .-> RWSW

    ITMRG -- "write new merged file<br/>+ build DataFile" --> IDFW
    ITMRG -- "reuses core's exact<br/>same merge-by-key semantics" --> PRM
    ICOMP -- "write new merged file<br/>+ build DataFile" --> IDFW
    ICOMP -- "combineFiles() only —<br/>no merge-by-key needed" --> PRM
    IVAC -- "locationOf() — resolve an<br/>expired DataFile's path for deletion" --> IDFW
    IVAC -- "table.expireSnapshots()" --> IT

    ILMC -. "sets Configuration's<br/>'lock-impl' property" .-> IDDBL
    IRC -. "Catalog passed to<br/>new (Catalog, TableIdentifier) ctor" .-> ITW
```

**The flow**: mirrors Delta's shape with Iceberg's own machinery underneath. `IcebergTableCreator` creates a brand-new table (via `iceberg-core`'s own `Table.create`, path-based or catalog-based), building the schema through `ArrowToIcebergSchema`. Once a table exists, `IcebergTableWriter` is the plain-append path — it hands the actual file-writing off to `IcebergDataFileWriter` (which writes the Parquet file and builds the matching Iceberg `DataFile` reference), then commits via `iceberg-core`'s own `Table.newAppend()`, which already has its own built-in optimistic-concurrency retry (unlike Delta's `CommitWriter`, no hand-rolled retry loop was needed here). `IcebergTableMerger` and `IcebergCompactor` both also go through `IcebergDataFileWriter` to produce their new file, then commit differently: `IcebergTableMerger` via `Table.newOverwrite()` (a real data change), reusing the exact same `PartitionRowMerger` merge-by-key logic Delta's `TableMerger` uses; `IcebergCompactor` via `Table.newRewrite()`, Iceberg's dedicated non-data-changing commit type. `IcebergVacuum` is a thin wrapper over Iceberg's own native `Table.expireSnapshots()` — Iceberg ships this directly, whereas Delta's `Vacuum` had to hand-roll the equivalent. `DynamoDbLockManager`/`IcebergLockManagerConfig` are the optional external-lock add-on (the same idea as Delta's `LockProvider`, implementing Iceberg's own `LockManager` extension point instead). `IcebergRestConfiguration`/`IcebergRestLocation` handle the alternate way of reaching a table — through a REST catalog service rather than a raw path — which `IcebergTableWriter` can use in place of a path-based constructor.

**`IcebergTableWriter` — confirmed broken** under genuine concurrent writers on real MinIO, root cause traced into `iceberg-core`'s own `HadoopTableOperations.renameToFinal`: the default `LockManager` (`InMemoryLockManager`) is a plain `HashMap` per-JVM — zero protection across separate processes — then `fs.rename()` (copy+delete on S3A, not atomic) lets every racing writer "succeed" with no conflict detected. MERGE confirmed separately broken too (4 of 8 concurrent inserts lost).

With lock (`DynamoDbLockManager`): **done, live-verified** — append *and* MERGE both pass with it engaged. Full write lifecycle (create → append → merge → compact → vacuum) proven end-to-end against real MinIO.

**`DynamoDbLockManager`**: the same `ResourceInUseException` bootstrap race fixed here as Delta's `DynamoDbLockProvider` — found via *this* class's own 8-separate-process test first, then confirmed Delta's lock had the identical unprotected `createTable` call. Scope deliberately smaller than Delta's lock: a lease-based steal, not a crash-recovery system — Iceberg's own `SnapshotProducer.commit()` retry loop already handles everything else.

**`IcebergRestConfiguration` — REST catalog + real MinIO, full data round trip: done, live-verified.** Solved with `CATALOG_WAREHOUSE=s3a://...` (not `s3://`) so the REST server's reported locations resolve against this project's own `MinioConfiguration`. Real live-only issues found and fixed along the way: container DNS resolving the MinIO hostname to its own loopback (fixed via `--add-host host-gateway`), and the container's JVM not trusting a local `mkcert` dev cert (fixed via `keytool`, run as root).
