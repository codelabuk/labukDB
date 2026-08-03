package com.labuk.deltaflight.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TableLocationTest {

    @Test
    void detectsRemoteUris() {
        assertTrue(TableLocation.isRemoteUri("s3a://bucket/table"));
        assertTrue(TableLocation.isRemoteUri("abfss://container@acct.dfs.core.windows.net/table"));
        assertTrue(TableLocation.isRemoteUri("hdfs://namenode:8020/table"));
    }

    @Test
    void treatsLocalPathsAsLocal() {
        assertFalse(TableLocation.isRemoteUri("/home/user/lakehouse/logs"));
        assertFalse(TableLocation.isRemoteUri("relative/path"));
        // A Windows drive letter must NOT be mistaken for a URI scheme —
        // "D:\..." has a colon but no "//", which is why the pattern
        // requires the full "scheme://" form rather than just a colon.
        assertFalse(TableLocation.isRemoteUri("D:\\repo\\lakehouse\\logs_delta"));
    }

    @Test
    void fromString_localPath_isLocal() {
        TableLocation location = TableLocation.fromString("/tmp/some-table");
        assertFalse(location.isRemote());
        assertEquals(Path.of("/tmp/some-table"), location.localPath());
    }

    @Test
    void fromString_remoteUri_isRemote() {
        TableLocation location = TableLocation.fromString("s3a://bucket/table");
        assertTrue(location.isRemote());
        assertEquals("s3a://bucket/table", location.raw());
        assertNotNull(location.hadoopConf(), "a remote location built via fromString gets a default Configuration");
    }

    @Test
    void resolve_preservesRemoteness_andJoinsCorrectly() {
        TableLocation remote = TableLocation.fromString("s3a://bucket/table");
        TableLocation dataFile = remote.resolve("level=ERROR/part-0.parquet");

        assertTrue(dataFile.isRemote());
        assertEquals("s3a://bucket/table/level=ERROR/part-0.parquet", dataFile.raw());
    }

    @Test
    void resolve_preservesLocalness() {
        TableLocation local = TableLocation.fromString("/tmp/some-table");
        TableLocation dataFile = local.resolve("level=ERROR/part-0.parquet");

        assertFalse(dataFile.isRemote());
        assertTrue(dataFile.raw().contains("part-0.parquet"));
    }

    @Test
    void accessingWrongAccessor_throwsRatherThanReturningNonsense() {
        TableLocation remote = TableLocation.fromString("s3a://bucket/table");
        TableLocation local = TableLocation.fromString("/tmp/some-table");

        assertThrows(IllegalStateException.class, remote::localPath);
        assertThrows(IllegalStateException.class, local::hadoopPath);
        assertThrows(IllegalStateException.class, local::hadoopConf);
    }

    @Test
    void forRemote_rejectsNonUri() {
        assertThrows(IllegalArgumentException.class, () ->
                TableLocation.forRemote("/not/a/uri", new org.apache.hadoop.conf.Configuration()));
    }
}
