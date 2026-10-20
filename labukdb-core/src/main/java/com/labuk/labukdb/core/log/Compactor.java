package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.action.AddFile;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Merges every currently-active file for one table + partition into a
 * single new file, via one atomic {@link CommitWriter#replaceFiles} call
 * — the periodic bin-packing step in the file-level LSM design (see
 * {@code docs/labukdb-v2-architecture.md}'s "Write path: it's an LSM
 * tree" section). {@link WriteBuffer}'s auto-flush bounds file count per
 * buffering cycle; this is what addresses file count accumulated
 * <em>across</em> cycles, over time.
 *
 * <b>Scope, stated plainly:</b> no size- or age-based selection policy —
 * every call merges <em>all</em> active files for the given partition
 * into one, regardless of whether some are already large. A real "only
 * merge files under N MB" heuristic is a named future refinement (a
 * performance/cost tuning), not required for this to be correct.
 * Compaction's commits don't introduce any new concurrency risk: the old
 * files' bytes stay on disk (only the log entries change), so a reader
 * that resolved its snapshot before this commit can still legally read
 * them — {@link Vacuum}, not {@code Compactor}, is what actually frees
 * disk space, deliberately later and separate.
 */
public final class Compactor {

    private final ParquetToArrowReader parquetReader = new ParquetToArrowReader();

    /**
     * @return the new table version, or -1 if fewer than 2 active files
     *         matched {@code partitionValues} — nothing meaningful to
     *         compact, not an error.
     */
    public long compact(Path tableRoot, Map<String, String> partitionValues, BufferAllocator allocator) throws IOException {
        SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
        Snapshot snapshot = snapshotManager.latest();

        List<AddFile> filesToCompact = snapshot.getActiveFiles().stream()
                .filter(f -> f.getPartitionValues().equals(partitionValues))
                .toList();

        if (filesToCompact.size() < 2) {
            return -1;
        }

        VectorSchemaRoot combined = null;
        try {
            for (AddFile file : filesToCompact) {
                File dataFile = tableRoot.resolve(file.getPath()).toFile();
                try (VectorSchemaRoot batch = parquetReader.read(dataFile, allocator, null, null, Map.of())) {
                    if (combined == null) {
                        combined = VectorSchemaRoot.create(batch.getSchema(), allocator);
                        combined.allocateNew();
                    }
                    VectorSchemaRootAppender.append(combined, batch);
                }
            }

            CommitWriter commitWriter = new CommitWriter(tableRoot);
            return commitWriter.replaceFiles(filesToCompact, combined, partitionValues);
        } finally {
            if (combined != null) {
                combined.close();
            }
        }
    }
}
