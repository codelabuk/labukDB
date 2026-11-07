<p align="center">
  <img alt="labukDB" src="docs/images/labukdb-lockup-light.svg" width="420">
</p>

<h4 align="center">A from-scratch Delta Lake &amp; Iceberg database — reads and writes, embeddable or served over Apache Arrow Flight.</h4>

<p align="center">
  <a href="https://github.com/codelabuk/labukDB/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/codelabuk/labukDB/ci.yml?style=for-the-badge&label=CI"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-14B8A6?style=for-the-badge"></a>
</p>

labukDB reads and writes both **Delta Lake** and **Apache Iceberg**
tables, built from scratch against each format's real on-disk protocol
— the transaction log, pruning, Parquet encode/decode, and the full
write path (append, upsert/MERGE by key, compaction, vacuum, table
creation, multi-writer concurrency), one small and independently
testable layer at a time. Use it embedded, in-process, with no server
(the DuckDB model), or run it as a standalone Arrow Flight server that
any language with a Flight client binding — Python, Java, Go, Rust, JS —
connects to unmodified (the Postgres model). A SQL layer sits on top of
both formats, including a real two-table join.

Writes aren't a demo feature: append, upsert/MERGE, compaction, vacuum,
multi-writer optimistic concurrency, and creating a brand-new table from
nothing are all implemented, tested, and **live-verified against a real
MinIO cluster**, not just a local-disk fixture.

**What makes this worth looking at**: real multi-writer correctness
research, with the failures kept visible rather than papered over. A
live stress test found this project's own remote commit path *silently
loses writes* under genuine concurrency on a real (non-compliant)
S3-compatible deployment — root-caused precisely, fixed with an opt-in
external lock, and proven fixed against the same live cluster. A second
stress test found Iceberg's own upstream `HadoopTables` writer has the
*identical class* of bug, also root-caused down to the exact library
code and the exact storage-layer assumption that doesn't hold on object
storage. Both findings, and the fixes, are documented in full in
`docs/ARCHITECTURE.md`'s concurrency section.

For the full architecture, see **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.
For every CLI flag, SQL subset, and Flight wire-format field, see
**[docs/USAGE.md](docs/USAGE.md)**.

---

## Quick start

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

Produces `labukdb-cli/target/labukdb-cli.jar` and
`labukdb-server-app/target/labukdb-server-app.jar` (the standalone
Flight server), each a single file with every dependency bundled.

```
bin/labukdb catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/labukdb query --sql "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"
```

See **[docs/USAGE.md](docs/USAGE.md)** for the Windows wrapper, running
the Flight server, and every other flag, SQL construct, and wire-format
field.

---

## Built on

| Library | Used for |
|---|---|
| [Apache Parquet](https://parquet.apache.org/) | Footer statistics, row-group reads, the `Group` record API |
| [Apache Arrow](https://arrow.apache.org/) | The in-memory columnar format every decoded row lands in |
| [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) | The gRPC-based wire protocol `flight/` serves over |
| [Apache Hadoop](https://hadoop.apache.org/) (`FileSystem` API only) | Storage abstraction — no MapReduce/YARN/HDFS daemons |
| [Jackson](https://github.com/FasterXML/jackson) | Parsing/writing `_delta_log` JSON commits |

---

*Copyright 2026 Afzal and labukDB contributors.
Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).*
