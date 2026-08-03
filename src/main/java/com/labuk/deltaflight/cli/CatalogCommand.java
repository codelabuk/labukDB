package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.catalog.TableCatalog;

import java.util.Map;

/**
 * {@code delta-cli catalog add <name> <path-or-uri>}
 * {@code delta-cli catalog list}
 * {@code delta-cli catalog remove <name>}
 *
 * The location can be a local path or a remote URI (e.g. {@code s3a://bucket/table})
 * - passed straight through as a String, never coerced into a
 * java.nio.file.Path here (that coercion is exactly the bug TableCatalog's
 * String-based rewrite fixed).
 */
final class CatalogCommand {

    private CatalogCommand() {
    }

    static void run(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
            return;
        }

        TableCatalog catalog = TableCatalog.atDefaultLocation();

        switch (args[0]) {
            case "add":
                if (args.length != 3) {
                    System.err.println("Usage: delta-cli catalog add <name> <path-or-uri>");
                    System.exit(1);
                    return;
                }
                catalog.register(args[1], args[2]);
                System.out.println("Registered '" + args[1] + "' -> " + args[2]);
                break;

            case "remove":
                if (args.length != 2) {
                    System.err.println("Usage: delta-cli catalog remove <name>");
                    System.exit(1);
                    return;
                }
                boolean removed = catalog.unregister(args[1]);
                System.out.println(removed ? "Removed '" + args[1] + "'" : "No entry named '" + args[1] + "'");
                break;

            case "list":
                Map<String, String> all = catalog.listAll();
                if (all.isEmpty()) {
                    System.out.println("No tables registered. Try: delta-cli catalog add <name> <path-or-uri>");
                } else {
                    all.forEach((name, location) -> System.out.println(name + "\t" + location));
                }
                break;

            default:
                printUsage();
                System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("""
                Usage:
                  delta-cli catalog add <name> <path-or-uri>
                  delta-cli catalog list
                  delta-cli catalog remove <name>
                """);
    }
}
