# labukDB — Module Dependency Overview

```mermaid
flowchart TB
    classDef coreModule fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef icebergModule fill:#D9F2C4,stroke:#534AB7,color:#26215C
    classDef storageNode fill:#9FE1CB,stroke:#0F6E56,color:#04342C
    classDef externalNode fill:#D3D1C7,stroke:#993C1D,color:#4A1B0C
    classDef actorNode fill:#FAECE7,stroke:#993C1D,color:#4A1B0C

    SB["Spark batch/streaming"]:::externalNode
    FL["Flink"]:::externalNode
    OIW["Other Iceberg writer<br/>(Trino, etc.)"]:::externalNode

    DT[("Delta table<br/>(_delta_log/ + Parquet)")]:::storageNode
    IT[("Iceberg table<br/>(metadata/ + Parquet)")]:::storageNode

    SB --> DT
    FL --> DT
    OIW --> IT

    subgraph CORE["labukdb-core (embeddable library, DuckDB-style — no Flight/gRPC dependency)"]
        P_STORAGE["storage/ — location dispatch<br/>(local/MinIO/S3/ADLS)"]
        P_FORMAT["format/ — TableFormat SPI<br/>(format-agnostic snapshot shape)"]
        P_LOG["log/ — Delta commit log<br/>+ write path"]
        P_PLANNER["planner/ — pruning"]
        P_DECODE["decode/ — Parquet → Arrow"]
        P_ENCODE["encode/ — Arrow → Parquet"]
        P_EXECUTION["execution/ — query operators<br/>(aggregate/sort/limit/combine/join)"]
        P_QUERY["query/ — SQL text → SqlQuery"]
        P_LOGICAL["logical/ — LogicalPlan IR<br/>+ optimizer (SQL engine)"]
        P_CATALOG["catalog/ — name → location"]
    end
    class CORE coreModule

    subgraph ICEMOD["labukdb-format-iceberg (optional for core embedders; required by cli/flight below)"]
        ICE_CLASSES["IcebergTableFormat / Writer /<br/>Creator / Merger / Compactor /<br/>Vacuum / REST catalog /<br/>DynamoDbLockManager"]
    end
    class ICEMOD icebergModule

    subgraph FLIGHTMOD["labukdb-flight (Arrow Flight server producer)"]
        DFP["LabukDbFlightProducer"]
    end
    class FLIGHTMOD coreModule

    subgraph SERVERMOD["labukdb-server-app (bootstrap, shaded jar)"]
        DFSA["LabukDbServerApp"]
    end
    class SERVERMOD coreModule

    subgraph CLIMOD["labukdb-cli (in-process, no server)"]
        CLI_CLASSES["LabukDbCli / QueryCommand /<br/>CatalogCommand / DescribeCommand"]
    end
    class CLIMOD coreModule

    DT --> P_LOG
    IT --> ICE_CLASSES

    ICEMOD -- depends on --> CORE
    FLIGHTMOD -- depends on --> CORE
    FLIGHTMOD -- depends on --> ICEMOD
    SERVERMOD -- depends on --> FLIGHTMOD
    CLIMOD -- depends on --> CORE
    CLIMOD -- depends on --> ICEMOD

    PY["Python<br/>(pyarrow.flight)"]:::actorNode
    JV["Java<br/>(arrow-flight-core)"]:::actorNode
    GX["Go/Rust/JS<br/>(any Flight client)"]:::actorNode
    SH["Shell<br/>(bin/labukdb)"]:::actorNode

    PY --> DFSA
    JV --> DFSA
    GX --> DFSA
    DFSA --> DFP
    SH --> CLI_CLASSES
```

**Legend**: lavender boxes are `labukdb-core`/`flight`/`server-app`/`cli`; green is `labukdb-format-iceberg`; teal cylinders are real storage (Delta/Iceberg tables); peach boxes are external actors (writers or clients).

**Dependency direction, the one rule that shapes this whole layout**: it runs *one way* — `core` never depends on `format-iceberg`. This is why format detection (`DetectedFormat`) lives in `core`, but actually *constructing* an `IcebergTableFormat` happens in `cli`/`flight` — the first layer that can see both modules. `core.logical.LogicalPlanExecutor` follows the same rule: it takes an already-opened `TableFormat` rather than detecting or opening the table itself.

**Two query paths inside `cli/`, both real**: `--table`/`--filter`/`--columns` (flag-based, unchanged since v1) and `--sql "<SELECT...>"` (the SQL engine — logical plan IR, optimizer, joins). Both end up calling the exact same `execution/` operators underneath.
