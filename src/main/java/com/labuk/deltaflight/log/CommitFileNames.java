package com.labuk.deltaflight.log;

import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * Delta commit files are named {@code NNNNNNNNNNNNNNNNNNNN.json} — a
 * 20-digit zero-padded version number. This exists so lexicographic sort
 * order (which is what most filesystems and object stores give you for
 * free when listing a directory) is also numeric version order, with no
 * need to parse filenames just to sort them correctly.
 */
final class CommitFileNames {

    private static final int VERSION_WIDTH = 20;
    private static final Pattern COMMIT_JSON = Pattern.compile("\\d{20}\\.json");

    private CommitFileNames() {
    }

    static String jsonFileName(long version) {
        return String.format("%0" + VERSION_WIDTH + "d.json", version);
    }

    static boolean isCommitJson(Path path) {
        return COMMIT_JSON.matcher(path.getFileName().toString()).matches();
    }

    static long parseVersion(Path path) {
        String name = path.getFileName().toString();
        return Long.parseLong(name.substring(0, VERSION_WIDTH));
    }
}
