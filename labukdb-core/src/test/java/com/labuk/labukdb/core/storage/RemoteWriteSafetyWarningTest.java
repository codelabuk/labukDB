package com.labuk.labukdb.core.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Not a log-content test (no test-scope SLF4J capture wired in this
 * project) — what actually matters, and what a regression here would
 * silently break for every local writer construction: {@link
 * RemoteWriteSafetyWarning#enforce} must short-circuit on
 * {@code !location.isRemote()} *before* ever calling {@link
 * TableLocation#hadoopConf()}, which throws {@link IllegalStateException}
 * for a local location. Also exercises every remote branch (explicit
 * MinIO-style endpoint unacknowledged/acknowledged, default-chain remote,
 * lock-configured remote) for the fail-closed-by-default behavior this
 * class now implements.
 */
class RemoteWriteSafetyWarningTest {

    @Test
    void localLocation_neverThrows_eventThoughHadoopConfWouldFailForIt() {
        TableLocation local = TableLocation.forLocal(Path.of("some", "table"));
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.enforce(local, false, false, "Delta"));
    }

    @Test
    void remoteLocation_withExplicitS3aEndpoint_noLock_notAcknowledged_throws() {
        Configuration conf = MinioConfiguration.forMinio("http://localhost:9000", "key", "secret");
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", conf);
        assertThrows(UnsafeRemoteWriteException.class,
                () -> RemoteWriteSafetyWarning.enforce(remote, false, false, "Delta"));
    }

    @Test
    void remoteLocation_withExplicitS3aEndpoint_noLock_acknowledged_doesNotThrow() {
        Configuration conf = MinioConfiguration.forMinio("http://localhost:9000", "key", "secret");
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", conf);
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.enforce(remote, false, true, "Delta"));
    }

    @Test
    void remoteLocation_withDefaultChainConfig_doesNotThrow_evenUnacknowledged() {
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", new Configuration());
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.enforce(remote, false, false, "Delta"));
    }

    @Test
    void remoteLocation_withLockConfigured_doesNotThrow_evenUnacknowledged() {
        Configuration conf = MinioConfiguration.forMinio("http://localhost:9000", "key", "secret");
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", conf);
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.enforce(remote, true, false, "Iceberg"));
    }
}
