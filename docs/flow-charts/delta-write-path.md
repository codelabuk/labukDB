# labukDB — Delta Write Path (`log/`)

```mermaid
flowchart TB
    classDef coreModule fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef storageNode fill:#9FE1CB,stroke:#0F6E56,color:#04342C

    DT[("Delta table<br/>(_delta_log/ + Parquet)")]:::storageNode

    TCR["TableCreator<br/>(version 0: protocol + metaData<br/>only, no add actions)"]:::coreModule
    ADS["ArrowToDeltaSchema<br/>(Arrow Schema → schemaString;<br/>List/Map/Struct all self-similar<br/>or nested-JSON-object shaped)"]:::coreModule
    ATPW["ArrowToParquetWriter<br/>(Arrow → real Parquet file)"]:::coreModule

    CW["CommitWriter<br/>(atomic create-if-absent commit —<br/>optimistic concurrency, retry on conflict)"]:::coreModule
    TMRG["TableMerger<br/>(upsert/MERGE by key,<br/>dataChange=true)"]:::coreModule
    PRM["PartitionRowMerger<br/>(format-agnostic merge-by-key<br/>algorithm — shared with Iceberg's<br/>own merger, not duplicated)"]:::coreModule
    COMP["Compactor<br/>(bin-pack small files,<br/>dataChange=false)"]:::coreModule
    VAC["Vacuum<br/>(delete orphaned files<br/>past retention window)"]:::coreModule

    LP["LockProvider (interface)"]:::coreModule
    DDBL["DynamoDbLockProvider<br/>(opt-in cross-process lock)"]:::coreModule
    RWSW["RemoteWriteSafetyWarning<br/>(logs on construction of any<br/>remote, no-lock writer)"]:::coreModule

    TCR -- "build schemaString" --> ADS
    TCR -- "write version 0" --> DT

    CW -- "append add/remove actions" --> DT
    TMRG -- "combineFiles() + merge()" --> PRM
    TMRG -- "commit via replaceFiles<br/>(dataChange=true)" --> CW
    COMP -- "commit bin-packed files<br/>(dataChange=false)" --> CW
    VAC -- "delete files past<br/>retired-and-expired" --> DT
    PRM -- "write new merged<br/>partition file" --> ATPW

    CW -. "optional, via<br/>forLocation(location, lockProvider)" .-> DDBL
    CW -. "construction —<br/>warns if remote + no lock" .-> RWSW
    DDBL -. implements .-> LP
```

**`CommitWriter` — confirmed broken** (real 8-process stress test against real MinIO, not assumed): remote, no lock — plain append *and* `TableMerger`/`Compactor` both lose commits silently under genuine concurrency. MERGE is the worse failure mode (7 of 8 inserts lost, zero exceptions) since its commit unit replaces the whole partition, not just adds a file.

Remote, **with** lock (`DynamoDbLockProvider`): **done, live-verified** — the same failing scenarios pass with it engaged. `TableMerger`/`Compactor` gained their own lock-accepting overloads specifically to close this (previously only plain `appendBatch` could opt in).

Local disk: safe unconditionally, lock never needed (real atomic filesystem ops).

**`DynamoDbLockProvider` — root cause** (not the lock's design, the thing it mitigates): S3-compatible storage's conditional PUT isn't always honored under concurrency the way local disk's `CREATE_NEW` is. DynamoDB's own conditional `PutItem` *is* proven atomic (8-thread test: 1 success, 7 clean conflicts) — chosen because it mirrors real Delta Lake OSS's own `S3DynamoDBLogStore` pattern, not invented.

Real regression fixed: 8 processes racing `ensureTableExists()` against a brand-new lock table all hit `ResourceNotFoundException` then raced `createTable` — 6 of 8 got an uncaught `ResourceInUseException`. Fixed by catching it and falling through to the existing poll-until-ACTIVE loop.
