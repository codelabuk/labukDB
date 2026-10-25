package com.labuk.labukdb.core.format;

import com.labuk.labukdb.core.storage.TableLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link DetectedFormat#detect} probes for the one directory each
 * format's protocol requires at the table root — no catalog metadata or
 * file-content parsing involved.
 */
class DetectedFormatTest {

    @Test
    void deltaLogDirectory_detectsAsDelta(@TempDir Path tableRoot) throws Exception {
        Files.createDirectories(tableRoot.resolve("_delta_log"));

        assertEquals(DetectedFormat.DELTA, DetectedFormat.detect(TableLocation.forLocal(tableRoot)));
    }

    @Test
    void metadataDirectory_detectsAsIceberg(@TempDir Path tableRoot) throws Exception {
        Files.createDirectories(tableRoot.resolve("metadata"));

        assertEquals(DetectedFormat.ICEBERG, DetectedFormat.detect(TableLocation.forLocal(tableRoot)));
    }

    @Test
    void neitherDirectory_rejectedWithClearError(@TempDir Path tableRoot) {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> DetectedFormat.detect(TableLocation.forLocal(tableRoot)));
        assertTrue(thrown.getMessage().contains("_delta_log") && thrown.getMessage().contains("metadata"),
                thrown.getMessage());
    }

    @Test
    void bothDirectoriesPresent_deltaWins(@TempDir Path tableRoot) throws Exception {
        // Not a real-world case (a table root wouldn't legitimately be both),
        // but the precedence should be deterministic, not accidental.
        Files.createDirectories(tableRoot.resolve("_delta_log"));
        Files.createDirectories(tableRoot.resolve("metadata"));

        assertEquals(DetectedFormat.DELTA, DetectedFormat.detect(TableLocation.forLocal(tableRoot)));
    }
}
