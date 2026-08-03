package com.labuk.deltaflight.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Delta commit files are named {@code NNNNNNNNNNNNNNNNNNNN.json} - a
 * 20-digit zero-padded version number, so lexicographic sort order (free
 * from any filesystem or object store listing) is also numeric version
 * order. Checkpoint files follow a related but distinct convention.
 *
 * Operates on plain filename Strings, not any specific Path type -
 * deliberately, so both {@link LocalNioLogStorage} (java.nio.file) and
 * {@link HadoopLogStorage} (Hadoop's Path) can share this logic without
 * either implementation depending on the other's Path flavor.
 *
 * <b>Checkpoint scope:</b> only single-part checkpoints
 * ({@code NNNN...N.checkpoint.parquet}) are recognized. Multi-part
 * checkpoints ({@code NNNN...N.checkpoint.0000000001.0000000003.parquet},
 * used once a table's checkpoint would otherwise exceed a size threshold)
 * are deliberately NOT matched here - {@link CheckpointReader} needs to
 * know the difference so it can fail loudly on a multi-part checkpoint
 * rather than silently reading only one part of the table's true live set.
 */
final class CommitFileNames {

    private static final int VERSION_WIDTH = 20;
    private static final Pattern COMMIT_JSON = Pattern.compile("(\\d{20})\\.json");
    private static final Pattern CHECKPOINT_SINGLE = Pattern.compile("(\\d{20})\\.checkpoint\\.parquet");
    private static final Pattern CHECKPOINT_MULTI = Pattern.compile("(\\d{20})\\.checkpoint\\.\\d{10}\\.\\d{10}\\.parquet");

    private CommitFileNames() {
    }

    static String jsonFileName(long version) {
        return String.format("%0" + VERSION_WIDTH + "d.json", version);
    }

    static boolean isCommitJson(String fileName) {
        return COMMIT_JSON.matcher(fileName).matches();
    }

    static long parseVersion(String fileName) {
        Matcher m = COMMIT_JSON.matcher(fileName);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a commit JSON filename: " + fileName);
        }
        return Long.parseLong(m.group(1));
    }

    static boolean isSinglePartCheckpoint(String fileName) {
        return CHECKPOINT_SINGLE.matcher(fileName).matches();
    }

    static boolean isMultiPartCheckpoint(String fileName) {
        return CHECKPOINT_MULTI.matcher(fileName).matches();
    }

    static long parseCheckpointVersion(String fileName) {
        Matcher m = CHECKPOINT_SINGLE.matcher(fileName);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a single-part checkpoint filename: " + fileName);
        }
        return Long.parseLong(m.group(1));
    }
}
