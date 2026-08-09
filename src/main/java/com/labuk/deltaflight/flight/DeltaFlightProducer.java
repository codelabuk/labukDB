package com.labuk.deltaflight.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.deltaflight.catalog.TableCatalog;
import com.labuk.deltaflight.decode.ParquetToArrowReader;
import com.labuk.deltaflight.flight.query.FlightQueryRequest;
import com.labuk.deltaflight.flight.query.PredicateCodec;
import com.labuk.deltaflight.log.Snapshot;
import com.labuk.deltaflight.log.SnapshotManager;
import com.labuk.deltaflight.log.TableCache;
import com.labuk.deltaflight.log.action.AddFile;
import com.labuk.deltaflight.planner.Predicate;
import com.labuk.deltaflight.planner.PartitionValueSubstituter;
import com.labuk.deltaflight.planner.QueryPlanner;
import com.labuk.deltaflight.planner.ScanPlan;
import com.labuk.deltaflight.storage.AdlsConfiguration;
import com.labuk.deltaflight.storage.MinioConfiguration;
import com.labuk.deltaflight.storage.TableLocation;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
    private final QueryPlanner queryPlanner = new QueryPlanner();
    private final TableCache tableCache = new TableCache();
    private final TableCatalog catalog;
    private final Configuration storageConf;

    public DeltaFlightProducer(BufferAllocator allocator) {
        this(allocator, TableCatalog.atDefaultLocation(), mergedStorageConf());
    }

    /** Explicit-catalog constructor — mainly for tests that need an isolated catalog file. */
    public DeltaFlightProducer(BufferAllocator allocator, TableCatalog catalog) {
        this(allocator, catalog, mergedStorageConf());
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
        this.allocator = allocator;
        this.catalog = catalog;
        this.storageConf = storageConf;
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
        String raw;
        if (query.getTable() != null) {
            raw = catalog.resolve(query.getTable())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "No table registered in the catalog under the name '" + query.getTable() + "'"));
        } else if (query.getTableRoot() != null) {
            raw = query.getTableRoot();
        } else {
            throw new IllegalArgumentException(
                    "Query must set either 'table' (a catalog name) or 'tableRoot' (a raw path or URI)");
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

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        String ticketId = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        try {
            ScanTicket scanTicket = registry.lookup(ticketId);
            try (VectorSchemaRoot root = parquetReader.read(
                    scanTicket.dataFile, allocator, scanTicket.projectedColumns,
                    scanTicket.residualPredicate, scanTicket.partitionValues)) {
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
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
            registry.invalidate(ticketId);
        }
    }

    /**
     * Lists every table registered in the catalog. Deliberately lightweight
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

    /*
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
}
