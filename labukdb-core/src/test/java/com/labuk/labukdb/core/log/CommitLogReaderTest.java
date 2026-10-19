package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.RemoveFile;
import com.labuk.labukdb.core.log.action.SingleAction;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogReaderTest {

    private final CommitLogReader reader = new CommitLogReader();

    private Path fixture(String name) throws URISyntaxException {
        return Paths.get(getClass().getClassLoader().getResource(name).toURI());
    }

    @Test
    void parsesFirstCommit_createTablePlusTwoAdds() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000000.json"));

        assertEquals(5, actions.size(), "commitInfo + protocol + metaData + 2 adds");
    }

    @Test
    void firstCommit_hasExpectedActionTypesInOrder() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000000.json"));

        assertNotNull(actions.get(0).getCommitInfo(), "line 1 should be commitInfo");
        assertNotNull(actions.get(1).getProtocol(), "line 2 should be protocol");
        assertNotNull(actions.get(2).getMetaData(), "line 3 should be metaData");
        assertNotNull(actions.get(3).getAdd(), "line 4 should be an add");
        assertNotNull(actions.get(4).getAdd(), "line 5 should be an add");
    }

    @Test
    void parsesMetaData_schemaAndPartitionColumns() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000000.json"));
        var metaData = actions.get(2).getMetaData();

        assertEquals(List.of("level"), metaData.getPartitionColumns());
        assertTrue(metaData.getSchemaString().contains("event_time"),
                "schemaString should be preserved raw, not dropped or reparsed");
    }

    @Test
    void parsesAddFile_pathPartitionValuesAndStats() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000000.json"));
        AddFile errorFile = actions.get(3).getAdd();

        assertEquals("level=ERROR/part-00000-abc.snappy.parquet", errorFile.getPath());
        assertEquals("ERROR", errorFile.getPartitionValues().get("level"));
        assertEquals(18234, errorFile.getSize());
        assertTrue(errorFile.isDataChange());
        assertTrue(errorFile.getStats().contains("numRecords"),
                "stats should be preserved as raw JSON for the planner layer to parse lazily");
    }

    @Test
    void parsesRemoveFile_secondCommit() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000001.json"));

        assertEquals(2, actions.size());
        assertNotNull(actions.get(0).getCommitInfo());

        RemoveFile removed = actions.get(1).getRemove();
        assertNotNull(removed, "line 2 should be a remove");
        assertEquals("level=ERROR/part-00000-abc.snappy.parquet", removed.getPath());
        assertEquals(1732000100000L, removed.getDeletionTimestamp());
    }

    @Test
    void unpopulatedActionFieldsAreNull_notEmptyObjects() throws Exception {
        List<SingleAction> actions = reader.read(fixture("00000000000000000000.json"));
        SingleAction commitInfoLine = actions.get(0);

        assertNull(commitInfoLine.getAdd());
        assertNull(commitInfoLine.getRemove());
        assertNull(commitInfoLine.getMetaData());
        assertNull(commitInfoLine.getProtocol());
    }
}
