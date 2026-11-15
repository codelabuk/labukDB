package com.labuk.labukdb.core.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Delta commit files are named {@code NNNNNNNNNNNNNNNNNNNN.json} — a
 * 20-digit zero-padded version number, so lexicographic sort order (free
 * from any filesystem or object store listing) is also numeric version
 * order. Checkpoint files follow a related but distinct convention.
 *
 * Operates on plain filename Strings, not any specific Path type —
 * deliberately, so both {@link LocalNioLogStorage} (java.nio.file) and
 * {@link HadoopLogStorage} (Hadoop's Path) can share this logic without
 * either implementation depending on the other's Path flavor.
 *
 * <b>Checkpoint support:</b> both single-part
 * ({@code NNNN...N.checkpoint.parquet}) and multi-part
 * ({@code NNNN...N.checkpoint.0000000001.0000000003.parquet}) checkpoints
 * are recognised. {@link SnapshotManager} detects which kind it found and
 * delegates accordingly — single-part files are read directly by
 * {@link CheckpointReader}; multi-part files are each read by
 * {@link CheckpointReader} in part-number order and their actions merged,
 * after verifying the complete set of parts is present.
 */
final class CommitFileNames {

    private static final int VERSION_WIDTH = 20;
    private static final Pattern COMMIT_JSON = Pattern.compile("(\\d{20})\\.json");
    private static final Pattern CHECKPOINT_SINGLE = Pattern.compile("(\\d{20})\\.checkpoint\\.parquet");
    private static final Pattern CHECKPOINT_MULTI = Pattern.compile(
            "(\\d{20})\\.checkpoint\\.(\\d{10})\\.(\\d{10})\\.parquet");

    private CommitFileNames() {
    }

    /** The {@code _last_checkpoint} pointer file's fixed name — never version-numbered, always overwritten in place. */
    static final String LAST_CHECKPOINT = "_last_checkpoint";

    static String jsonFileName(long version) {
        return String.format("%0" + VERSION_WIDTH + "d.json", version);
    }

    /** Single-part only — {@link CheckpointWriter} never produces a multi-part checkpoint (see its own javadoc for why). */
    static String checkpointFileName(long version) {
        return String.format("%0" + VERSION_WIDTH + "d.checkpoint.parquet", version);
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

    static boolean isAnyCheckpoint(String fileName) {
        return isSinglePartCheckpoint(fileName) || isMultiPartCheckpoint(fileName);
    }

    static long parseCheckpointVersion(String fileName) {
        Matcher m = CHECKPOINT_SINGLE.matcher(fileName);
        if (m.matches()) {
            return Long.parseLong(m.group(1));
        }
        Matcher mm = CHECKPOINT_MULTI.matcher(fileName);
        if (mm.matches()) {
            return Long.parseLong(mm.group(1));
        }
        throw new IllegalArgumentException("Not a checkpoint filename: " + fileName);
    }

    /** For a multi-part checkpoint file, returns which part this is (1-based). */
    static int parseCheckpointPartNumber(String fileName) {
        Matcher m = CHECKPOINT_MULTI.matcher(fileName);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a multi-part checkpoint filename: " + fileName);
        }
        return Integer.parseInt(m.group(2));
    }

    /** For a multi-part checkpoint file, returns the total number of parts. */
    static int parseCheckpointTotalParts(String fileName) {
        Matcher m = CHECKPOINT_MULTI.matcher(fileName);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a multi-part checkpoint filename: " + fileName);
        }
        return Integer.parseInt(m.group(3));
    }
}
