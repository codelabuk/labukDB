package com.labuk.deltaflight.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.deltaflight.log.action.SingleAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a single Delta commit file - e.g. {@code _delta_log/00000000000000000004.json}
 * - into its list of actions.
 *
 * Delta's commit files are JSON-lines, not a single JSON document: each
 * line is independently parseable. This matters for two reasons beyond
 * style - it's why Delta commits are cheap to append to (no need to
 * rewrite a wrapping array), and it means a reader can, in principle,
 * process a commit file as a stream rather than loading it whole.
 *
 * One malformed line fails the whole commit rather than silently skipping
 * it: a partially-applied commit is a correctness bug, not a partial
 * result to shrug off.
 *
 * {@code read(Path)} is unchanged in behavior from before {@link LogStorage}
 * existed - it's kept as the direct entry point for local-path callers and
 * for {@code CommitLogReaderTest}. {@code readFrom(BufferedReader, String)}
 * factors out the actual parsing so {@link SnapshotManager} can reuse it
 * against any {@link LogStorage} backend's reader, local or remote,
 * without duplicating the parsing logic.
 */
public class CommitLogReader {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param commitFile path to a single {@code NNNNNNNNNNNNNNNNNNNN.json} commit file
     * @return the actions in that commit, in file order
     */
    public List<SingleAction> read(Path commitFile) {
        try (BufferedReader reader = Files.newBufferedReader(commitFile)) {
            return readFrom(reader, commitFile.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read commit file " + commitFile, e);
        }
    }

    /**
     * Parses commit JSON-lines from an already-open reader, regardless of
     * what storage backend produced it. {@code sourceDescription} is used
     * only for error messages (e.g. a filename or URI).
     */
    public List<SingleAction> readFrom(BufferedReader reader, String sourceDescription) {
        List<SingleAction> actions = new ArrayList<>();
        int lineNumber = 0;
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    actions.add(mapper.readValue(line, SingleAction.class));
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Malformed action at " + sourceDescription + ":" + lineNumber, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + sourceDescription, e);
        }
        return actions;
    }
}
