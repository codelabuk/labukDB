package com.labuk.labukdb.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.execution.AggregateOperator;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.CombinedQueryExecutor;
import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.flight.query.AggregateQueryCodec;
import com.labuk.labukdb.flight.query.FlightQueryRequest;
import com.labuk.labukdb.flight.query.PredicateCodec;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.TableCache;
import com.labuk.labukdb.core.log.WriteBuffer;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.PartitionValueSubstituter;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.planner.ScanPlan;
import com.labuk.labukdb.core.storage.AdlsConfiguration;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import com.labuk.labukdb.flight.query.FlightWriteRequest;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ties layers 1-3 together and exposes them over Arrow Flight.
 *
 * <b>{@code GetFlightInfo}</b> does all the planning: resolves the
 * requested snapshot (version, timestamp, or latest), runs partition and
 * file-stats pruning ({@link QueryPlanner}), and returns one
 * {@link FlightEndpoint} per surviving file — so a client can fetch
 * multiple files in parallel via multiple {@code DoGet} streams, rather
 * than this producer serializing everything through one connection.
 *
 * <b>{@code DoGet}</b> does the actual work for exactly one file: row-group
 * pruning, decode, and residual filtering, all inside
 * {@link ParquetToArrowReader}. It's lazy per endpoint — a client that never
 * calls {@code DoGet} for an endpoint never causes that file to be opened.
 *
 * Extends {@code NoOpFlightProducer} rather than implementing every
 * {@code FlightProducer} method by hand — {@code DoPut}, {@code DoAction},
 * {@code DoExchange} are genuinely out of scope for a read-only engine
 * right now, and {@code NoOpFlightProducer} gives them a correct
 * "unimplemented" response instead of this class guessing at signatures it
 * doesn't need. {@code ListFlights} IS implemented (see below) — it's the
 * one of the four that had a real, immediate use: listing catalog entries.
 */
public class DeltaFlightProducer extends NoOpFlightProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAllocator allocator;
    private final TicketRegistry registry = new TicketRegistry();
    private final ParquetToArrowReader parquetReader = new ParquetToArrowReader();
    private final CombinedQueryExecutor combinedQueryExecutor = new CombinedQueryExecutor();
    private final QueryPlanner queryPlanner = new QueryPlanner();
    private final TableCache tableCache = new TableCache();
    private final TableCatalog catalog;
    private final Configuration storageConf;
    private final Path walRoot;

    /**
     * Active {@link WriteBuffer}s, keyed by table root + sorted partition
     * values — reused across separate {@code DoPut} calls so buffering
     * actually accumulates instead of each call starting a fresh,
     * empty buffer (which would defeat the point of buffering at all).
     * Unbounded, no eviction — a first-cut simplification: an idle entry
     * holds no open file handles, only a small amount of heap, so this
     * isn't a correctness problem, just not a complete cache policy yet.
     */
    private final Map<String, WriteBuffer> writeBuffers = new ConcurrentHashMap<>();

    private static final long DEFAULT_MAX_BUFFERED_ROWS = 100_000;
    private static final long DEFAULT_MAX_BUFFERED_BYTES = 64L * 1024 * 1024;

    public DeltaFlightProducer(BufferAllocator allocator) {
        this(allocator, TableCatalog.atDefaultLocation(), mergedStorageConf(), DEFAULT_WAL_ROOT);
    }

    /** Explicit-catalog constructor — mainly for tests that need an isolated catalog file. */
    public DeltaFlightProducer(BufferAllocator allocator, TableCatalog catalog) {
        this(allocator, catalog, mergedStorageConf(), DEFAULT_WAL_ROOT);
    }

    /**
     * Merges MinIO and ADLS environment-variable config into one
     * Configuration so a single server instance can serve tables from
     * either storage backend without the caller having to pick one.
     * Explicit settings from either helper win over Hadoop's defaults;
     * if both set the same key (unlikely — they use distinct prefixes)
     * MinIO's value wins, since it's iterated last.
     */
    private static Configuration mergedStorageConf() {
        Configuration conf = AdlsConfiguration.fromEnvironment();
        MinioConfiguration.fromEnvironment().iterator()
                .forEachRemaining(entry -> conf.set(entry.getKey(), entry.getValue()));
        return conf;
    }

    /**
     * @param storageConf Hadoop configuration used for any remote
     *                    ({@code s3a://}, {@code abfss://}) table location —
     *                    endpoint, credentials, path-style access. See
     *                    {@link MinioConfiguration}. Unused for local tables.
     */
    public DeltaFlightProducer(BufferAllocator allocator, TableCatalog catalog, Configuration storageConf) {
        this(allocator, catalog, storageConf, DEFAULT_WAL_ROOT);
    }

    /**
     * Explicit {@code walRoot} — mainly for tests that need an isolated
     * WAL staging directory instead of the real {@code ~/.labukdb/wal}
     * default, same reasoning as the explicit-catalog constructor above.
     */
    public DeltaFlightProducer(BufferAllocator allocator, TableCatalog catalog, Configuration storageConf, Path walRoot) {
        this.allocator = allocator;
        this.catalog = catalog;
        this.storageConf = storageConf;
        this.walRoot = walRoot;
    }

    /**
     * Resolves {@code table} (a catalog name) first if present, otherwise
     * falls back to the raw {@code tableRoot} path — the original form,
     * still fully supported, not deprecated by this.
     */
    /**
     * Resolves {@code table} (a catalog name) first if present, otherwise
     * falls back to the raw {@code tableRoot}. Returns a
     * {@link TableLocation}, which decides local-vs-remote once, here, so
     * everything downstream (log reads, data-file reads) follows the same
     * decision rather than each re-detecting a URI scheme.
     *
     * Remote locations get this producer's {@code storageConf} — supplied
     * at construction (see {@link #DeltaFlightProducer(BufferAllocator,
     * TableCatalog, Configuration)}), which is where MinIO/S3 endpoint and
     * credentials come from.
     */
    private TableLocation resolveTableRoot(FlightQueryRequest query) {
        return resolveTableLocation(query.getTable(), query.getTableRoot());
    }

    /**
     * Shared by {@code getFlightInfo}/{@code getSchema} (via
     * {@link #resolveTableRoot}) and {@code acceptPut} — resolves exactly
     * one of a catalog name or a raw path/URI to a {@link TableLocation},
     * with remote locations picking up this producer's {@code storageConf}.
     */
    private TableLocation resolveTableLocation(String table, String tableRoot) {
        String raw;
        if (table != null) {
            raw = catalog.resolve(table)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "No table registered in the catalog under the name '" + table + "'"));
        } else if (tableRoot != null) {
            raw = tableRoot;
        } else {
            throw new IllegalArgumentException(
                    "Request must set either 'table' (a catalog name) or 'tableRoot' (a raw path or URI)");
        }
        return TableLocation.isRemoteUri(raw)
                ? TableLocation.forRemote(raw, storageConf)
                : TableLocation.fromString(raw);
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            FlightQueryRequest query = MAPPER.readValue(descriptor.getCommand(), FlightQueryRequest.class);
            TableLocation tableRoot = resolveTableRoot(query);
            SnapshotManager snapshotManager = tableCache.forTable(tableRoot);

            Snapshot snapshot;
            if (query.getVersion() != null) {
                snapshot = snapshotManager.atVersion(query.getVersion());
            } else if (query.getTimestampMillis() != null) {
                snapshot = snapshotManager.atTimestamp(Instant.ofEpochMilli(query.getTimestampMillis()));
            } else {
                snapshot = snapshotManager.latest();
            }

            Predicate predicate = PredicateCodec.fromJson(query.getPredicate());
            ScanPlan plan = queryPlanner.plan(snapshot, predicate, query.getColumns());

            List<String> groupByColumns = query.getGroupBy() == null ? List.of() : query.getGroupBy();
            List<AggregateSpec> aggregates = AggregateQueryCodec.aggregatesFromJson(query.getAggregates());
            List<SortKey> sortKeys = AggregateQueryCodec.orderByFromJson(query.getOrderBy());
            long limit = query.getLimit() == null ? -1 : query.getLimit();
            boolean combineMode = !groupByColumns.isEmpty() || !aggregates.isEmpty()
                    || !sortKeys.isEmpty() || limit >= 0;

            if (combineMode) {
                return combinedFlightInfo(descriptor, tableRoot, plan, groupByColumns, aggregates, sortKeys, limit);
            }

            List<FlightEndpoint> endpoints = new ArrayList<>();
            for (AddFile file : plan.getFilesToScan()) {
                TableLocation dataFile = tableRoot.resolve(file.getPath());
                // The plan's residual predicate is the whole original
                // predicate, still possibly referencing partition columns
                // (e.g. "level = 'ERROR' AND amount > 2"). Each file's own
                // partition values are known exactly, so any partition-
                // column clause folds to a boolean constant here — this is
                // what makes it safe to hand the result to decode, which
                // only knows about this file's physical columns.
                Predicate perFileResidual =
                        PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());
                String ticketId = registry.register(new ScanTicket(
                        dataFile, plan.getProjectedColumns(), perFileResidual, file.getPartitionValues()));
                endpoints.add(new FlightEndpoint(new Ticket(ticketId.getBytes(StandardCharsets.UTF_8))));
            }

            // Schema is inferred from the first surviving file's own footer
            // plus that file's partition values, rather than parsed from
            // Delta's schemaString — that string is Spark's JSON schema
            // encoding, a separate format this engine doesn't parse; the
            // physical Parquet file (plus known partition columns) is
            // authoritative for what decode/ actually produces anyway. If
            // pruning eliminates every file, there's nothing to infer a
            // schema from — an empty Schema is returned rather than guessing.
            Schema schema = plan.getFilesToScan().isEmpty()
                    ? new Schema(List.of())
                    : parquetReader.readSchema(
                    tableRoot.resolve(plan.getFilesToScan().get(0).getPath()),
                    plan.getProjectedColumns(),
                    plan.getFilesToScan().get(0).getPartitionValues());

            return FlightInfo.builder(schema, descriptor, endpoints)
                    .setRecords(-1) // unknown without opening every file; not worth the cost just to report it
                    .setBytes(-1)
                    .build();
        } catch (IOException e) {
            throw CallStatus.INTERNAL.withDescription("Failed to plan query: " + e.getMessage()).toRuntimeException();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage()).toRuntimeException();
        } catch (RuntimeException e) {
            // Catches things like UncheckedIOException from SnapshotManager
            // (e.g. a table root that doesn't resolve to a real _delta_log
            // directory) that don't match either narrower catch above.
            // Without this, the exception propagates uncaught and Flight's
            // default handling replaces it with a generic, useless
            // "There was an error servicing your request" — including the
            // real exception type and message here is the difference
            // between a diagnosable error and a dead end. Logged
            // server-side too, since the server console otherwise shows
            // nothing at all when a request fails — only the client sees
            // an error.
            e.printStackTrace();
            throw CallStatus.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
        }
    }

    /**
     * Combine-mode planning — any of {@code groupBy}/{@code aggregates}/
     * {@code orderBy}/{@code limit} present. Unlike the plain-scan path,
     * every surviving file must be read together before a result exists,
     * so this registers exactly one {@link CombinedScanTicket} covering
     * all of them and returns exactly one {@link FlightEndpoint} — the
     * literal fix for {@code GetFlightInfo}'s one-file-per-endpoint model
     * not fitting cross-file combination.
     */
    private FlightInfo combinedFlightInfo(FlightDescriptor descriptor, TableLocation tableRoot, ScanPlan plan,
                                           List<String> groupByColumns, List<AggregateSpec> aggregates,
                                           List<SortKey> sortKeys, long limit) throws IOException {
        boolean aggregating = !groupByColumns.isEmpty() || !aggregates.isEmpty();

        // Decode needs every column referenced anywhere in this query --
        // not just query.getColumns(), which doesn't apply the same way
        // once the output is defined by groupBy/aggregates instead of a
        // free projection.
        Set<String> neededColumns = new LinkedHashSet<>();
        if (aggregating) {
            neededColumns.addAll(groupByColumns);
            for (AggregateSpec spec : aggregates) {
                neededColumns.add(spec.column());
            }
        } else {
            if (plan.getProjectedColumns() != null) {
                neededColumns.addAll(plan.getProjectedColumns());
            }
            // A plain orderBy/limit (no grouping) sorts on raw columns --
            // make sure one is available to sort by even if the client
            // didn't think to request it explicitly.
            for (SortKey key : sortKeys) {
                neededColumns.add(key.column());
            }
        }
        List<String> projectedColumns = List.copyOf(neededColumns);

        List<FileScan> files = new ArrayList<>();
        for (AddFile file : plan.getFilesToScan()) {
            TableLocation dataFile = tableRoot.resolve(file.getPath());
            Predicate perFileResidual =
                    PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());
            files.add(new FileScan(dataFile, perFileResidual, file.getPartitionValues()));
        }

        List<FlightEndpoint> endpoints = new ArrayList<>();
        if (!files.isEmpty()) {
            String ticketId = registry.register(new CombinedScanTicket(
                    files, projectedColumns, groupByColumns, aggregates, sortKeys, limit));
            endpoints.add(new FlightEndpoint(new Ticket(ticketId.getBytes(StandardCharsets.UTF_8))));
        }

        // Same "no surviving files -> empty schema" convention the plain-
        // scan path already has -- aggregate output types can't be known
        // without inspecting at least one file's physical schema.
        Schema schema;
        if (plan.getFilesToScan().isEmpty()) {
            schema = new Schema(List.of());
        } else if (aggregating) {
            AddFile firstFile = plan.getFilesToScan().get(0);
            Set<String> inputColumns = new LinkedHashSet<>(groupByColumns);
            for (AggregateSpec spec : aggregates) {
                inputColumns.add(spec.column());
            }
            Schema inputSchema = parquetReader.readSchema(
                    tableRoot.resolve(firstFile.getPath()), List.copyOf(inputColumns), firstFile.getPartitionValues());

            List<org.apache.arrow.vector.types.pojo.Field> outputFields = new ArrayList<>();
            for (String column : groupByColumns) {
                outputFields.add(inputSchema.findField(column));
            }
            for (AggregateSpec spec : aggregates) {
                outputFields.add(AggregateOperator.outputFieldFor(spec, inputSchema));
            }
            schema = new Schema(outputFields);
        } else {
            AddFile firstFile = plan.getFilesToScan().get(0);
            schema = parquetReader.readSchema(
                    tableRoot.resolve(firstFile.getPath()), projectedColumns, firstFile.getPartitionValues());
        }

        return FlightInfo.builder(schema, descriptor, endpoints)
                .setRecords(-1)
                .setBytes(-1)
                .build();
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        String ticketId = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        // Only ever non-null mid-pipeline in the CombinedScanTicket branch
        // -- tracked here so a failure there still closes the executor's
        // result, via the finally block below, without risking a
        // double-close on the success path (set back to null right after
        // a clean stream).
        VectorSchemaRoot current = null;
        try {
            FlightTicket flightTicket = registry.lookup(ticketId);

            if (flightTicket instanceof ScanTicket scanTicket) {
                try (VectorSchemaRoot root = parquetReader.read(
                        scanTicket.dataFile, allocator, scanTicket.projectedColumns,
                        scanTicket.residualPredicate, scanTicket.partitionValues)) {
                    listener.start(root);
                    listener.putNext();
                    listener.completed();
                }
                return;
            }

            CombinedScanTicket combinedTicket = (CombinedScanTicket) flightTicket;
            current = combinedQueryExecutor.execute(combinedTicket.files, combinedTicket.projectedColumns,
                    combinedTicket.groupByColumns, combinedTicket.aggregates, combinedTicket.sortKeys,
                    combinedTicket.limit, allocator);

            listener.start(current);
            listener.putNext();
            listener.completed();
            current.close();
            current = null;
        } catch (IOException e) {
            listener.error(CallStatus.INTERNAL.withDescription("Failed to read file: " + e.getMessage())
                    .toRuntimeException());
        } catch (IllegalArgumentException e) {
            listener.error(CallStatus.NOT_FOUND.withDescription(e.getMessage()).toRuntimeException());
        } catch (RuntimeException e) {
            // Same reasoning as getFlightInfo's catch-all: an uncaught type
            // here becomes an opaque generic Flight error instead of
            // something a client (or you, reading the log) can act on.
            e.printStackTrace();
            listener.error(CallStatus.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException());
        } finally {
            if (current != null) {
                current.close();
            }
            registry.invalidate(ticketId);
        }
    }

    /**
     * Streams incoming Arrow batches into a {@link WriteBuffer} for the
     * requested table + partition, flushing unconditionally once the
     * client finishes sending — a successful {@code DoPut} means
     * "committed," not "buffered somewhere, maybe." Batching across
     * *separate* {@code DoPut} calls via {@code WriteBuffer}'s own
     * size/time threshold isn't exercised by this policy (only batching
     * *within* one call's multiple streamed batches is) — a deliberate
     * v1 simplification, not an oversight.
     *
     * Local or remote (S3/MinIO/ADLS) — {@code CommitWriter}/
     * {@code ArrowToParquetWriter}/{@code WriteBuffer} are all
     * {@code TableLocation}-dispatched now; live-MinIO-verified for
     * remote. The table itself must already exist — {@code DoPut} only
     * appends, same as {@code CommitWriter.appendBatch}; creating a new
     * table is {@code TableCreator}, not exposed over Flight yet.
     */
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                FlightWriteRequest request = MAPPER.readValue(
                        flightStream.getDescriptor().getCommand(), FlightWriteRequest.class);
                TableLocation tableRoot = resolveTableLocation(request.getTable(), request.getTableRoot());

                Map<String, String> partitionValues = request.getPartitionValues();
                if (partitionValues == null) {
                    throw new IllegalArgumentException(
                            "'partitionValues' is required — WriteBuffer buffers one partition at a time");
                }

                WriteBuffer writeBuffer = writeBufferFor(tableRoot, partitionValues);

                while (flightStream.next()) {
                    writeBuffer.append(flightStream.getRoot());
                }
                if (writeBuffer.bufferedRowCount() > 0) {
                    writeBuffer.flush();
                }

                ackStream.onNext(PutResult.empty());
                ackStream.onCompleted();
            } catch (IOException e) {
                ackStream.onError(CallStatus.INTERNAL.withDescription("Failed to write: " + e.getMessage())
                        .toRuntimeException());
            } catch (IllegalArgumentException | IllegalStateException e) {
                ackStream.onError(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage()).toRuntimeException());
            } catch (RuntimeException e) {
                // Same reasoning as getFlightInfo's catch-all.
                e.printStackTrace();
                ackStream.onError(CallStatus.INTERNAL
                        .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                        .withCause(e)
                        .toRuntimeException());
            }
        };
    }

    /**
     * Keyed by table root + sorted partition values, so repeated
     * {@code DoPut} calls against the same (table, partition) reuse one
     * buffering {@link WriteBuffer} instead of each starting fresh.
     *
     * Defaults to {@code ~/.labukdb/wal} — same convention
     * {@code TableCatalog.atDefaultLocation()} already uses for
     * {@code ~/.labukdb/catalog.json}. The WAL stays local even for a
     * remote table (see {@code WriteBuffer}'s class javadoc for why).
     */
    private static final Path DEFAULT_WAL_ROOT =
            Path.of(System.getProperty("user.home"), ".labukdb", "wal");

    private WriteBuffer writeBufferFor(TableLocation tableRoot, Map<String, String> partitionValues) {
        String key = tableRoot.raw() + "|" + new TreeMap<>(partitionValues);
        return writeBuffers.computeIfAbsent(key, k -> {
            try {
                return new WriteBuffer(tableRoot, walRoot, partitionValues,
                        DEFAULT_MAX_BUFFERED_ROWS, DEFAULT_MAX_BUFFERED_BYTES, allocator);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to open WriteBuffer for " + tableRoot, e);
            }
        });
    }

    /**
     * Returns the schema for a named table without planning or scanning
     * any data — used by the NL query validator to build a column
     * whitelist before generating predicates, and by any client that
     * needs to discover table structure cheaply.
     *
     * Arrow Flight's {@code GetSchema} RPC is separate from
     * {@code GetFlightInfo} precisely for this use case: schema discovery
     * without committing to a full scan plan. {@code NoOpFlightProducer}
     * inherits it as a no-op; this override makes it real.
     */
    @Override
    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
        try {
            FlightQueryRequest query = MAPPER.readValue(descriptor.getCommand(), FlightQueryRequest.class);
            TableLocation tableRoot = resolveTableRoot(query);
            SnapshotManager snapshotManager = tableCache.forTable(tableRoot);
            Snapshot snapshot = snapshotManager.latest();

            if (snapshot.getActiveFiles().isEmpty()) {
                return new SchemaResult(new Schema(List.of()));
            }

            AddFile sample = snapshot.getActiveFiles().iterator().next();
            TableLocation dataFile = tableRoot.resolve(sample.getPath());
            Schema schema = parquetReader.readSchema(dataFile, null, sample.getPartitionValues());
            return new SchemaResult(schema);
        } catch (IOException e) {
            throw CallStatus.INTERNAL.withDescription("Failed to read schema: " + e.getMessage()).toRuntimeException();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage()).toRuntimeException();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw CallStatus.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
        }
    }

    /**
     * Deliberately lightweight
     * compared to {@code GetFlightInfo} — no snapshot resolution, no
     * pruning, no per-file endpoints. This answers "what tables exist,"
     * not "here's a plan to scan one." A client still calls
     * {@code GetFlightInfo} with a {@code table} name afterward to actually
     * query one.
     *
     * {@code criteria} (Flight's filter-by-bytes mechanism for narrowing a
     * listing) isn't used here — genuinely not needed at catalog sizes this
     * is meant for; revisit if that stops being true.
     */
    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        try {
            catalog.listAll().forEach((name, tableRoot) -> {
                FlightDescriptor descriptor = FlightDescriptor.path(name);
                FlightInfo info = FlightInfo.builder(new Schema(List.of()), descriptor, List.of())
                        .setRecords(-1)
                        .setBytes(-1)
                        .build();
                listener.onNext(info);
            });
            listener.onCompleted();
        } catch (RuntimeException e) {
            e.printStackTrace();
            listener.onError(CallStatus.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException());
        }
    }
}
