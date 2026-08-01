package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.planner.Predicate;

import java.time.Instant;
import java.util.List;

final class CliArgs {

    String tableRoot;
    List<String> columns;
    Predicate predicate;
    Long version;
    Instant asOf;
    int limit = 50;

    static CliArgs parse(String[] args) {
        CliArgs result = new CliArgs();
        String filterExpr = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--table":
                    result.tableRoot = args[++i];
                    break;
                case "--columns":
                    result.columns = List.of(args[++i].split(","));
                    break;
                case "--filter":
                    filterExpr = args[++i];
                    break;
                case "--version":
                    result.version = Long.parseLong(args[++i]);
                    break;
                case "--as-of":
                    result.asOf = Instant.parse(args[++i]);
                    break;
                case "--limit":
                    result.limit = Integer.parseInt(args[++i]);
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    return null;
            }
        }

        if (result.tableRoot == null) {
            System.err.println("--table is required");
            return null;
        }
        if (result.version != null && result.asOf != null) {
            System.err.println("--version and --as-of are mutually exclusive");
            return null;
        }

        result.predicate = filterExpr != null ? SimpleFilterParser.parse(filterExpr) : null;
        return result;
    }

    static void printUsage() {
        System.err.println("""
                Usage: DeltaCli --table <path> [options]

                Options:
                  --columns col1,col2      project only these columns (default: all)
                  --filter "expr"          e.g. "level=ERROR AND amount>2" (AND only, see SimpleFilterParser)
                  --version N              query as of a specific version
                  --as-of TIMESTAMP        query as of an ISO-8601 instant (e.g. 2026-07-17T00:00:00Z)
                  --limit N                cap printed rows, 0 = unlimited (default 50)
                """);
    }
}
