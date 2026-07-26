package com.labuk.deltaflight.log;

import com.labuk.deltaflight.log.action.AddFile;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotManagerTest {

    // Fixture: version 0 creates the table with two adds (ERROR, INFO partitions).
    // Version 1 removes the ERROR file. See src/test/resources/sample-table/_delta_log.
    private static final Instant V0_TIMESTAMP = Instant.ofEpochMilli(1732000000000L);
    private static final Instant V1_TIMESTAMP = Instant.ofEpochMilli(1732000100000L);

    private Path tableRoot() throws URISyntaxException {
        return Paths.get(getClass().getClassLoader().getResource("sample-table").toURI());
    }

    private List<String> pathsOf(java.util.Collection<AddFile> files) {
        return files.stream().map(AddFile::getPath).sorted().collect(Collectors.toList());
    }

    @Test
    void versionZero_hasBothFiles() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());
        Snapshot snapshot = manager.atVersion(0);

        assertEquals(0, snapshot.getVersion());
        assertEquals(2, snapshot.getActiveFileCount());
        assertTrue(pathsOf(snapshot.getActiveFiles()).stream().anyMatch(p -> p.contains("level=ERROR")));
        assertTrue(pathsOf(snapshot.getActiveFiles()).stream().anyMatch(p -> p.contains("level=INFO")));
    }

    @Test
    void versionOne_errorFileRemoved() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());
        Snapshot snapshot = manager.atVersion(1);

        assertEquals(1, snapshot.getVersion());
        assertEquals(1, snapshot.getActiveFileCount());
        assertTrue(pathsOf(snapshot.getActiveFiles()).stream().anyMatch(p -> p.contains("level=INFO")));
        assertFalse(pathsOf(snapshot.getActiveFiles()).stream().anyMatch(p -> p.contains("level=ERROR")),
                "removed file must not appear in the live set, even though the physical file still exists on disk");
    }

    @Test
    void latest_resolvesToHighestVersion() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());
        Snapshot snapshot = manager.latest();

        assertEquals(1, snapshot.getVersion());
        assertEquals(1, snapshot.getActiveFileCount());
    }

    @Test
    void snapshotIsUnaffectedByLaterManagerCalls() throws Exception {
        // This is the isolation property: a Snapshot handed to a caller must
        // not change even after the manager's internal cache moves forward.
        SnapshotManager manager = new SnapshotManager(tableRoot());
        Snapshot oldSnapshot = manager.atVersion(0);

        manager.atVersion(1); // advances the manager's internal cache

        assertEquals(2, oldSnapshot.getActiveFileCount(),
                "a previously-obtained Snapshot must remain exactly as it was when resolved");
    }

    @Test
    void movingBackwardAfterCachingForward_stillCorrect() throws Exception {
        // Exercises the "full replay on backward move" path described in
        // SnapshotManager's caching-model javadoc.
        SnapshotManager manager = new SnapshotManager(tableRoot());

        manager.atVersion(1); // cache now sits at version 1 (1 active file)
        Snapshot backToZero = manager.atVersion(0); // must NOT just be "1 file minus nothing"

        assertEquals(0, backToZero.getVersion());
        assertEquals(2, backToZero.getActiveFileCount(),
                "resolving an older version after caching a newer one must still see the file that was later removed");
    }

    @Test
    void requestingUncommittedVersion_throws() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());

        assertThrows(IllegalArgumentException.class, () -> manager.atVersion(5));
    }

    @Test
    void metadataCarriesForwardAcrossCommitsThatDontRedeclareIt() throws Exception {
        // Version 1's commit has no metaData action at all — the schema from
        // version 0 must still be visible in the version-1 snapshot.
        SnapshotManager manager = new SnapshotManager(tableRoot());
        Snapshot snapshot = manager.atVersion(1);

        assertNotNull(snapshot.getMetadata(), "metaData from an earlier commit must carry forward");
        assertEquals(List.of("level"), snapshot.getMetadata().getPartitionColumns());
    }

    @Test
    void atTimestamp_resolvesToVersionActiveAtThatTime() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());

        Snapshot atV0Time = manager.atTimestamp(V0_TIMESTAMP);
        assertEquals(0, atV0Time.getVersion());

        Snapshot justBeforeV1 = manager.atTimestamp(V1_TIMESTAMP.minusMillis(1));
        assertEquals(0, justBeforeV1.getVersion(),
                "a timestamp before v1's commit must still resolve to v0");

        Snapshot atV1Time = manager.atTimestamp(V1_TIMESTAMP);
        assertEquals(1, atV1Time.getVersion());
    }

    @Test
    void atTimestamp_beforeTableExisted_throws() throws Exception {
        SnapshotManager manager = new SnapshotManager(tableRoot());

        assertThrows(IllegalArgumentException.class,
                () -> manager.atTimestamp(V0_TIMESTAMP.minusSeconds(60)));
    }
}
