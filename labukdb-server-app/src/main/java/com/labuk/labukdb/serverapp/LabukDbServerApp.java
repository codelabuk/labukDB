package com.labuk.labukdb.serverapp;

import com.labuk.labukdb.flight.LabukDbFlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Entry point. {@code java -cp ... LabukDbServerApp [port]}, default
 * port 9443.
 *
 * A client's {@code GetFlightInfo} command should point at a specific
 * table root (see {@code FlightQueryRequest}) — this server itself is
 * table-agnostic; it resolves whatever {@code tableRoot} each request asks
 * for, rather than being started against one fixed table.
 */
public final class LabukDbServerApp {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9443;

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            LabukDbFlightProducer producer = new LabukDbFlightProducer(allocator);
            Location location = Location.forGrpcInsecure("0.0.0.0", port);

            try (FlightServer server = FlightServer.builder(allocator, location, producer).build()) {
                server.start();
                System.out.println("labukdb listening on " + location.getUri());
                server.awaitTermination();
            }
        }
    }
}
