package com.labuk.deltaflight.cli;

import java.util.Arrays;

/**
 * Entry point — routes to a subcommand, kubectl-style, rather than being
 * one flat flag set. {@code catalog}/{@code query}/{@code describe} each
 * live in their own class; this class only dispatches.
 *
 * <pre>
 *   delta-cli catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
 *   delta-cli catalog list
 *   delta-cli describe logs
 *   delta-cli query --table logs --filter "level=ERROR" --columns service,message
 * </pre>
 *
 * Every subcommand accepts either a catalog name or a raw path anywhere a
 * table is expected — see {@link TableResolver}.
 */
public final class DeltaCli {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
            return;
        }

        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "catalog":
                CatalogCommand.run(rest);
                break;
            case "query":
                QueryCommand.run(rest);
                break;
            case "describe":
                DescribeCommand.run(rest);
                break;
            default:
                System.err.println("Unknown subcommand: " + args[0]);
                printUsage();
                System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("""
                Usage: delta-cli <subcommand> [options]

                Subcommands:
                  catalog add <name> <path>   register a table under a short name
                  catalog list                show registered tables
                  catalog remove <name>       unregister a table
                  describe <name-or-path>     schema, partition columns, file count
                  query --table <name-or-path> [opts]  run a filtered/projected query — see QueryCommand
                """);
    }
}
