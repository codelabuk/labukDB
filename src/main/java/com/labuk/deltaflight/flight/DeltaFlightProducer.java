package com.labuk.deltaflight.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * {@code FlightProducer} method by hand: {@code ListFlights}, {@code DoPut},
 * {@code DoAction}, {@code DoExchange} are all genuinely out of scope for a
 * read-only single-table-at-a-time engine right now, and NoOpFlightProducer
 * gives them a correct "unimplemented" response instead of this class
 * guessing at signatures it doesn't need.
 */
public class DeltaFlightProducer extends NoOpFlightProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAllocator allocator;
    private final TicketRegistry registry = new TicketRegistry();
    private final ParquetToArrowReader parquetReader = new ParquetToArrowReader();
    private final QueryPlanner queryPlanner = new QueryPlanner();
    private final TableCache tableCache = new TableCache();

    public DeltaFlightProducer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            FlightQueryRequest query = MAPPER.readValue(descriptor.getCommand(), FlightQueryRequest.class);
            Path tableRoot = Paths.get(query.getTableRoot());
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
                File parquetFile = tableRoot.resolve(file.getPath()).toFile();
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
                        parquetFile, plan.getProjectedColumns(), perFileResidual, file.getPartitionValues()));
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
                    tableRoot.resolve(plan.getFilesToScan().get(0).getPath()).toFile(),
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
                    scanTicket.parquetFile, allocator, scanTicket.projectedColumns,
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
}
