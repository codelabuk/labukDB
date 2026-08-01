package com.labuk.deltaflight.cli;

import com.labuk.deltaflight.catalog.TableCatalog;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * {@code delta-cli catalog add <name> <path>}
 * {@code delta-cli catalog list}
 * {@code delta-cli catalog remove <name>}
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
                    System.err.println("Usage: delta-cli catalog add <name> <path>");
                    System.exit(1);
                    return;
                }
                catalog.register(args[1], Paths.get(args[2]));
                System.out.println("Registered '" + args[1] + "' -> " + Paths.get(args[2]).toAbsolutePath());
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
                Map<String, Path> all = catalog.listAll();
                if (all.isEmpty()) {
                    System.out.println("No tables registered. Try: delta-cli catalog add <name> <path>");
                } else {
                    all.forEach((name, path) -> System.out.println(name + "\t" + path));
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
                  delta-cli catalog add <name> <path>
                  delta-cli catalog list
                  delta-cli catalog remove <name>
                """);
    }
}
