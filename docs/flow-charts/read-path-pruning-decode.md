# labukDB — Read Path: Pruning (`planner/`) + Decode (`decode/`)

```mermaid
flowchart TB
    classDef coreModule fill:#EEEDFE,stroke:#534AB7,color:#26215C

    SNAP["Snapshot / NormalizedSnapshot<br/>(live file set + schema,<br/>from log/ or format/)"]:::coreModule

    subgraph PLANNER["planner/ — pruning (no file opened yet)"]
        QP["QueryPlanner<br/>(plan() for Delta's Snapshot;<br/>planNormalized() for any<br/>TableFormat's NormalizedSnapshot)"]
        PP["PartitionPruner<br/>(① exact match on partition<br/>values from the log — no I/O)"]
        FSP["FileStatsPruner<br/>(② range check on add.stats<br/>min/max from the log — no I/O;<br/>Iceberg's DataFileRef.getStats()<br/>is always null today — no<br/>column-stats pruning for Iceberg yet)"]
        PVS["PartitionValueSubstituter<br/>(folds partition predicate<br/>clauses to constants per file)"]
        PC["PredicateColumns<br/>(collects columns referenced<br/>in the predicate tree)"]
        SCANPLAN["ScanPlan / NormalizedScanPlan<br/>(surviving files + residual<br/>predicate + skip count)"]
    end
    class PLANNER coreModule

    subgraph DECODE["decode/ — Parquet → Arrow (file now opened)"]
        PTAR["ParquetToArrowReader<br/>(local File or remote<br/>Hadoop InputFile)"]
        RGP["RowGroupPruner<br/>(③ Parquet footer stats —<br/>one file open, header only)"]
        GPE["GroupPredicateEvaluator<br/>(④ per-row residual filter,<br/>fused with decode — failing<br/>rows never enter Arrow vectors)"]
        PVA["PartitionValueAppender<br/>(fills partition columns as<br/>constants — not in the<br/>physical Parquet file)"]
        NTD["NestedTypeDecoder<br/>(LIST/MAP/STRUCT, any nesting<br/>depth — read side)"]
        ITC["Int96TimestampCodec<br/>(legacy Spark timestamp<br/>encoding → Instant UTC)"]
        DCC["DecimalCodec<br/>(Decimal128, precision≤38;<br/>shared with encode/)"]
        PFC["ParquetFooterCache<br/>(schema cached per stable<br/>local path)"]
    end
    class DECODE coreModule

    SNAP --> QP
    QP -- "① partition values" --> PP
    QP -- "② file stats min/max" --> FSP
    QP -- "fold partition predicates<br/>per file, before decode" --> PVS
    QP -- "collect predicate columns<br/>for safe projection" --> PC
    QP --> SCANPLAN

    SCANPLAN -- "one call per<br/>surviving file" --> PTAR

    PTAR -- "③ skipNextRowGroup()<br/>for pruned row groups" --> RGP
    PTAR -- "④ residual filter,<br/>fused with decode" --> GPE
    PTAR -- "reconstruct partition<br/>columns as constants" --> PVA
    PTAR -- "decode LIST/MAP/STRUCT<br/>columns" --> NTD
    PTAR -- "decode INT96 timestamps" --> ITC
    PTAR -- "decode DECIMAL<br/>(4 legal physical encodings)" --> DCC
    PTAR -- "cache schema by path<br/>(local only)" --> PFC
```

**Four pruning levels, cheapest first** — ① and ② never open a file at all; ③ opens one file's footer only; ④ is fused with decode itself, zero extra materialization.

**`NestedTypeDecoder`**: a recursive dispatcher — any depth of LIST/MAP/STRUCT nesting, four leaf primitive kinds (float/double/int32/int64/string). Decimal/timestamp/boolean leaves inside a nested position are a separate, not-yet-wired expansion — deliberately, to avoid a schema that builds but a reader that can't actually decode it.

**`ParquetToArrowReader` — the projection-pushdown fix this project found the hard way**: the physical read set is OUTPUT columns UNION predicate columns (via `PredicateColumns.collect`) — a `WHERE` on a column not in the `SELECT` list used to throw `InvalidRecordException` before this, since the predicate referenced a column never decoded.
