# labukDB — Catalog Resolution + Format Detection

```mermaid
flowchart TB
    classDef coreModule fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef icebergModule fill:#D9F2C4,stroke:#534AB7,color:#26215C

    RAW["Raw table reference<br/>(--table arg, SQL FROM-clause name,<br/>or Flight request's 'table' field)"]

    subgraph CATPKG["catalog/ (labukdb-core)"]
        CAT["TableCatalog<br/>(name → location string only —<br/>NO format field; ~/.labukdb/catalog.json,<br/>Jackson-backed, rewritten on every<br/>register/unregister)"]
    end
    class CATPKG coreModule

    subgraph CLIPKG["cli/ (labukdb-cli)"]
        TR2["TableResolver<br/>(catalog name, raw path, or<br/>remote URI — catalog lookup wins<br/>if it matches, else used literally)"]
        IRL["IcebergRestLocation<br/>(icebergrest://ns.table —<br/>checked BEFORE TableResolver,<br/>since isRemoteUri's regex would<br/>otherwise misroute it to Hadoop FS)"]
    end
    class CLIPKG coreModule

    subgraph STORAGEPKG["storage/ (labukdb-core)"]
        TL["TableLocation<br/>(local vs. remote, decided once;<br/>fromStringWithEnvironmentConfig —<br/>auto-picks up MinIO/ADLS connector<br/>config from env for an absolute URI)"]
        CONF["MinioConfiguration /<br/>AdlsConfiguration<br/>(fromEnvironment())"]
    end
    class STORAGEPKG coreModule

    subgraph FORMATPKG["format/ (labukdb-core)"]
        DF["DetectedFormat<br/>(probes exists('_delta_log')<br/>vs exists('metadata') at the<br/>table root — lazy, not stored<br/>in the catalog)"]
        TF["TableFormat<br/>(interface: latest/atVersion/<br/>atTimestamp → NormalizedSnapshot)"]
        DTF["DeltaTableFormat"]
    end
    class FORMATPKG coreModule

    subgraph ICEPKG["labukdb-format-iceberg"]
        ITF["IcebergTableFormat<br/>(path-based or Catalog-based)"]
        IRC["IcebergRestConfiguration<br/>(ICEBERG_REST_* env vars<br/>→ RESTCatalog)"]
    end
    class ICEPKG icebergModule

    RAW -- "is it icebergrest://... ?" --> IRL
    IRL -- "yes — connect,<br/>parse TableIdentifier" --> IRC
    IRC -- "new IcebergTableFormat(<br/>catalog, identifier)" --> ITF

    RAW -- "no — resolve as<br/>catalog name or path" --> TR2
    TR2 -- "catalog.resolve(name)<br/>.orElse(name)" --> CAT
    TR2 -- "isRemoteUri? →<br/>forRemote(uri, conf) :<br/>fromString(raw)" --> TL
    TL --> CONF

    TL -- "detect(tableRoot)" --> DF
    DF -- "_delta_log/ found" --> DTF
    DF -- "metadata/ found" --> ITF

    DTF -. implements .-> TF
    ITF -. implements .-> TF
```

**`TableCatalog` — deliberate interim limitation**, stated plainly in its own javadoc: no format tag stored per entry. Nothing stops a Delta table and an Iceberg table from being registered under different names side by side — each consumer independently detects format at query time via `DetectedFormat`, not at registration time.

**`DeltaTableFormat` — real gap found and fixed (SQL engine Phase 1)**: it existed, unit-tested standalone, but had *only* a local-only `Path` constructor — `QueryCommand`'s actual Delta query path bypassed it entirely in favor of `SnapshotManager`/`Snapshot`/`AddFile` directly (confirmed by grep: zero references). Added `DeltaTableFormat(TableLocation)`, delegating to the already-remote-capable `SnapshotManager.forLocation` — the constructor migrating the query path onto it needed first, or MinIO Delta SQL queries would have silently broken.

Still true after the fix: Flight and cli's flag-based `--table` path still bypass `DeltaTableFormat` for Delta (unchanged, out of scope) — only cli's `--sql` path uses it, via `core.logical.LogicalPlanExecutor`.
