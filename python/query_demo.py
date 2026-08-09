"""
Minimal proof that DeltaFlightServerApp is usable from any language with an
Arrow Flight client binding — nothing about the server changes to support
this; Flight IS gRPC, so this just connects like any other gRPC client.

Requires: pip install pyarrow

Usage:
    python query_demo.py <host:port> <table_root> [predicate_json]

Examples:
    # Everything in the table
    python query_demo.py localhost:9443 D:\\repo\\local-lakehouse-lab\\lakehouse\\logs_delta

    # Filtered — matches PredicateCodec's JSON wire format exactly
    python query_demo.py localhost:9443 D:\\repo\\local-lakehouse-lab\\lakehouse\\logs_delta \
        "{\\"type\\":\\"cmp\\",\\"column\\":\\"level\\",\\"op\\":\\"EQ\\",\\"literal\\":\\"ERROR\\",\\"literalType\\":\\"string\\"}"
"""

import sys
import json

import pyarrow.flight as flight


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    location = f"grpc://{sys.argv[1]}"
    table_root = sys.argv[2]

    # Special mode: --schema only prints the table schema and exits.
    # Useful for the NL query validator to discover column names.
    if "--schema" in sys.argv:
        client = flight.FlightClient(location)
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps({"table": table_root}).encode("utf-8"))
        schema = client.get_schema(descriptor).schema
        print(f"Schema for '{table_root}':")
        for field in schema:
            print(f"  {field.name}: {field.type}")
        return

    query = {"tableRoot": table_root}
    if len(sys.argv) > 3:
        try:
            query["predicate"] = json.loads(sys.argv[3])
        except json.JSONDecodeError as e:
            print(f"Could not parse the predicate argument as JSON: {e}")
            print()
            print("JSON requires double quotes, not single quotes. In bash/Git Bash, "
                  "wrap the WHOLE argument in single quotes (untouched literally) and "
                  "use double quotes inside it for the JSON itself, e.g.:")
            print("""  '{"type":"cmp","column":"level","op":"EQ","literal":"ERROR","literalType":"string"}'""")
            sys.exit(1)

    client = flight.FlightClient(location)
    descriptor = flight.FlightDescriptor.for_command(json.dumps(query).encode("utf-8"))

    info = client.get_flight_info(descriptor)
    print(f"Schema: {info.schema}")
    print(f"Endpoints (files planned to scan): {len(info.endpoints)}")
    print()

    total_rows = 0
    for i, endpoint in enumerate(info.endpoints):
        reader = client.do_get(endpoint.ticket)
        table = reader.read_all()
        total_rows += table.num_rows
        print(f"--- endpoint {i} ({table.num_rows} rows) ---")
        print(table)
        print()

    print(f"Total rows: {total_rows}")


if __name__ == "__main__":
    main()
