# labukDB — Flight Query Round Trip

```mermaid
sequenceDiagram
    actor C as Client (Python / Java / any Flight client)
    participant DFP as LabukDbFlightProducer
    participant DF as IcebergRestLocation / DetectedFormat
    participant TF as TableFormat (DeltaTableFormat path unused here — see note)
    participant QP as QueryPlanner
    participant TR as TicketRegistry
    participant EXEC as CombinedQueryExecutor / ParquetToArrowReader

    Note over C,EXEC: GetFlightInfo (planning — no data file fully opened)

    C->>DFP: GetFlightInfo(FlightDescriptor)<br/>{"table":"logs","predicate":{...},<br/>"columns":[...],"groupBy":[...],<br/>"aggregates":[...],"orderBy":[...],"limit":N}
    DFP->>DF: icebergrest:// reference?<br/>(checked BEFORE any TableLocation —<br/>isRemoteUri's regex would otherwise misroute it)

    alt REST-catalog Iceberg
        DFP->>TF: IcebergRestConfiguration.connect()<br/>+ new IcebergTableFormat(catalog, identifier)
    else path-based (Delta or Iceberg)
        DFP->>DF: DetectedFormat.detect(tableRoot)<br/>(exists("_delta_log") vs exists("metadata"))
        alt Delta
            DFP->>DFP: resolve via SnapshotManager/Snapshot<br/>directly — NOT via DeltaTableFormat<br/>(see note below)
        else Iceberg
            DFP->>TF: new IcebergTableFormat(tableRoot)
        end
    end

    DFP->>TF: latest() / atVersion() / atTimestamp()<br/>(Delta: SnapshotManager directly instead)
    TF-->>DFP: Snapshot / NormalizedSnapshot<br/>(live file set + schema + partition columns)

    DFP->>QP: plan(snapshot, predicate, columns)<br/>-- Delta --  or  planNormalized(...) -- Iceberg --

    rect rgb(241, 239, 232)
        Note over QP: Pruning (no file opened yet)
        QP->>QP: ① PartitionPruner — exact match<br/>on partition values from the log
        QP->>QP: ② FileStatsPruner — range check on<br/>file stats min/max from the log<br/>(Iceberg's DataFileRef.getStats() is<br/>always null today — partition pruning<br/>only, no column-stats pruning yet)
        QP-->>DFP: ScanPlan / NormalizedScanPlan<br/>(surviving files + residual predicate<br/>+ skip count)
    end

    DFP->>DFP: combineMode?<br/>(groupBy/aggregates/orderBy/limit present,<br/>Delta — Iceberg is ALWAYS combine mode,<br/>unlike cli's --sql path, which maps onto<br/>LogicalPlan/TableFormat instead — see<br/>sql-engine-logical-plan.md)

    alt combine mode
        DFP->>TR: register ONE CombinedScanTicket<br/>covering every surviving file
        TR-->>DFP: opaque ticket id
        DFP-->>C: FlightInfo — schema + ONE FlightEndpoint
    else plain scan (Delta only)
        loop for each surviving file
            DFP->>DFP: PartitionValueSubstituter —<br/>fold partition predicate clauses<br/>to constants per file
            DFP->>TR: register(ScanTicket)
            TR-->>DFP: opaque ticket id
        end
        DFP-->>C: FlightInfo — schema + N FlightEndpoints<br/>(client can fetch in parallel)
    end

    Note over C,EXEC: DoGet (one per endpoint)

    C->>DFP: DoGet(ticket)
    DFP->>TR: lookup(ticketId)
    TR-->>DFP: ScanTicket / CombinedScanTicket

    alt CombinedScanTicket
        DFP->>EXEC: CombinedQueryExecutor.execute(files,<br/>columns, groupBy, aggregates, orderBy, limit)
        EXEC->>EXEC: decode every file, concatenate,<br/>optionally aggregate, optionally sort+limit
    else ScanTicket
        DFP->>EXEC: ParquetToArrowReader.read(dataFile,<br/>columns, residualPredicate, partitionValues)
        EXEC->>EXEC: ③ RowGroupPruner (footer stats) —<br/>④ GroupPredicateEvaluator (per-row,<br/>fused with decode) — NestedTypeDecoder<br/>(LIST/MAP/STRUCT) — Int96TimestampCodec —<br/>DecimalCodec — PartitionValueAppender
    end

    EXEC-->>DFP: VectorSchemaRoot
    DFP-->>C: Arrow IPC stream<br/>(zero-copy — Flight wire format IS Arrow IPC)
    DFP->>TR: invalidate(ticketId)
```

**Delta's query path bypasses `DeltaTableFormat` here** — confirmed by grep: zero references to it in `LabukDbFlightProducer`. This is unchanged by the SQL engine work, which only wired `DeltaTableFormat` into cli's `--sql` path (see `sql-engine-logical-plan.md`). Mapping Flight's own query protocol onto the same `LogicalPlan`/`TableFormat` path is natural future work, not done yet.

**`DoPut` (write path, not shown above)**: Delta buffers via `WriteBuffer` (WAL-backed, flush on size/time threshold); Iceberg commits immediately, one `DoPut` = one Iceberg commit, no `WriteBuffer` equivalent. Both live-MinIO-verified.
