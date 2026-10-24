package com.labuk.labukdb.flight;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps an opaque ticket id (a random UUID) to the {@link ScanTicket} it
 * refers to.
 *
 * <b>A deliberate simplification worth naming, not hiding:</b> Arrow Flight
 * tickets are meant to be opaque bytes the client just echoes back — nothing
 * requires them to be a server-side lookup key. A self-describing ticket
 * (encode the file path, columns, and predicate directly into the ticket
 * bytes, e.g. as the same JSON shape {@code FlightQueryRequest} already
 * uses) would let this producer be stateless and safely run as more than
 * one instance behind a load balancer. This registry approach is simpler to
 * get correct first — it's the thing to replace if this ever needs to scale
 * horizontally, not a design to defend as final.
 */
final class TicketRegistry {

    private final Map<String, FlightTicket> tickets = new ConcurrentHashMap<>();

    String register(FlightTicket ticket) {
        String id = UUID.randomUUID().toString();
        tickets.put(id, ticket);
        return id;
    }

    FlightTicket lookup(String id) {
        FlightTicket ticket = tickets.get(id);
        if (ticket == null) {
            throw new IllegalArgumentException("Unknown or already-consumed ticket: " + id);
        }
        return ticket;
    }

    void invalidate(String id) {
        tickets.remove(id);
    }
}
