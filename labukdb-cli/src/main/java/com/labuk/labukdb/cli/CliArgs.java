package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.planner.Predicate;

import java.time.Instant;
import java.util.List;

final class CliArgs {

    String table;
    List<String> columns;
    Predicate predicate;
    Long version;
    Instant asOf;
    int limit = 50;
    String sql;

    static CliArgs parse(String[] args) {
        CliArgs result = new CliArgs();
        String filterExpr = null;
        boolean limitExplicit = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--table":
                    result.table = args[++i];
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
                    limitExplicit = true;
                    break;
                case "--sql":
                    result.sql = args[++i];
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    return null;
            }
        }

        if (result.sql != null) {
            if (result.table != null || result.columns != null || filterExpr != null || limitExplicit) {
                System.err.println("--sql is mutually exclusive with --table/--columns/--filter/--limit "
                        + "(the SQL text names the table and the SELECT/WHERE/LIMIT clauses subsume the rest)");
                return null;
            }
        } else if (result.table == null) {
            System.err.println("--table is required (a catalog name or a raw path)");
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
                Usage: LabukDbCli query --table <name-or-path> [options]
                       LabukDbCli query --sql "<SELECT statement>" [--version N | --as-of TIMESTAMP]

                --table can be either a name registered via 'LabukDbCli catalog add',
                or a raw filesystem path. With --sql, the table name comes from the
                SQL's own FROM clause instead and is resolved the same way.

                Options:
                  --columns col1,col2      project only these columns (default: all)
                  --filter "expr"          e.g. "level=ERROR AND amount>2" (AND only, see SimpleFilterParser)
                  --version N              query as of a specific version
                  --as-of TIMESTAMP        query as of an ISO-8601 instant (e.g. 2026-07-17T00:00:00Z)
                  --limit N                cap printed rows, 0 = unlimited (default 50)
                  --sql "<SELECT ...>"     run real SQL (SELECT/WHERE/GROUP BY/ORDER BY/LIMIT);
                                           mutually exclusive with --table/--columns/--filter/--limit;
                                           see SqlQueryTranslator for the exact supported subset
                """);
    }
}
