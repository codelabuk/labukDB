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
 * Parses a single Delta commit file — e.g. {@code _delta_log/00000000000000000004.json}
 * — into its list of actions.
 *
 * Delta's commit files are JSON-lines, not a single JSON document: each
 * line is independently parseable. This matters for two reasons beyond
 * style — it's why Delta commits are cheap to append to (no need to
 * rewrite a wrapping array), and it means a reader can, in principle,
 * process a commit file as a stream rather than loading it whole. This
 * implementation loads a commit fully into memory (commit files are one
 * transaction's worth of changes, not the whole table — they're small),
 * but keeps the line-by-line parse so a future streaming variant is a
 * small change, not a rewrite.
 *
 * One malformed line fails the whole commit rather than silently skipping
 * it: a partially-applied commit is a correctness bug, not a partial
 * result to shrug off.
 */
public class CommitLogReader {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param commitFile path to a single {@code NNNNNNNNNNNNNNNNNNNN.json} commit file
     * @return the actions in that commit, in file order (order matters: within a
     *         single commit, later lines can reference state established by earlier
     *         ones, though for add/remove/metaData order doesn't affect the result —
     *         it's preserved regardless, since correctness shouldn't depend on the
     *         caller trusting that assumption)
     */
    public List<SingleAction> read(Path commitFile) {
        List<SingleAction> actions = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(commitFile)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    actions.add(mapper.readValue(line, SingleAction.class));
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Malformed action at " + commitFile + ":" + lineNumber, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read commit file " + commitFile, e);
        }
        return actions;
    }
}
