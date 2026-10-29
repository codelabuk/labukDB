package com.labuk.labukdb.core.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Not a log-content test (no test-scope SLF4J capture wired in this
 * project) — what actually matters, and what a regression here would
 * silently break for every local writer construction: {@link
 * RemoteWriteSafetyWarning#warnIfUnsafe} must short-circuit on
 * {@code !location.isRemote()} *before* ever calling {@link
 * TableLocation#hadoopConf()}, which throws {@link IllegalStateException}
 * for a local location. Also exercises the remote branches (explicit
 * MinIO-style endpoint, default-chain remote, lock-configured remote) so
 * none of them throws either.
 */
class RemoteWriteSafetyWarningTest {

    @Test
    void localLocation_neverThrows_eventThoughHadoopConfWouldFailForIt() {
        TableLocation local = TableLocation.forLocal(Path.of("some", "table"));
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.warnIfUnsafe(local, false, "Delta"));
    }

    @Test
    void remoteLocation_withExplicitS3aEndpoint_doesNotThrow() {
        Configuration conf = MinioConfiguration.forMinio("http://localhost:9000", "key", "secret");
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", conf);
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.warnIfUnsafe(remote, false, "Delta"));
    }

    @Test
    void remoteLocation_withDefaultChainConfig_doesNotThrow() {
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", new Configuration());
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.warnIfUnsafe(remote, false, "Delta"));
    }

    @Test
    void remoteLocation_withLockConfigured_doesNotThrow() {
        Configuration conf = MinioConfiguration.forMinio("http://localhost:9000", "key", "secret");
        TableLocation remote = TableLocation.forRemote("s3a://bucket/table", conf);
        assertDoesNotThrow(() -> RemoteWriteSafetyWarning.warnIfUnsafe(remote, true, "Iceberg"));
    }
}
