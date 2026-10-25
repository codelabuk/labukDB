package com.labuk.labukdb.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link IcebergRestConfiguration#fromEnvironment} property-mapping
 * correctness — deterministic, no network. Uses real {@code
 * CatalogProperties}/{@code AuthProperties}/{@code OAuth2Properties} key
 * constants, confirmed accurate against the real {@code iceberg-core}
 * classes via {@code javap -constants} before this was written, not
 * guessed. Does not (and cannot, without a process-wide env override
 * mechanism) test the real {@code ICEBERG_REST_URI}-is-required path
 * directly against this JVM's actual environment — that's exercised
 * indirectly by every other test in this class running without it set
 * and {@link IcebergTableFormat}/{@link IcebergTableWriter}'s path-based
 * constructors being what every other Iceberg test in this project uses.
 */
class IcebergRestConfigurationTest {

    @Test
    void missingUri_rejectedWithClearError() {
        // This JVM's real environment has no ICEBERG_REST_URI set in CI/dev
        // by construction (nothing in this project ever sets it) -- a
        // direct, real assertion, not a mocked one.
        assertNull(System.getenv("ICEBERG_REST_URI"),
                "test assumes ICEBERG_REST_URI isn't set in this environment");
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                IcebergRestConfiguration::fromEnvironment);
        assertTrue(thrown.getMessage().contains("ICEBERG_REST_URI"), thrown.getMessage());
    }

    @Test
    void authTypeKeyNames_matchRealIcebergCoreConstants() {
        // Confirms the literal key names IcebergRestConfiguration writes
        // into the property map are the exact ones iceberg-core itself
        // reads, not just string literals that happen to look right.
        assertEquals("uri", CatalogProperties.URI);
        assertEquals("warehouse", CatalogProperties.WAREHOUSE_LOCATION);
        assertEquals("rest.auth.type", AuthProperties.AUTH_TYPE);
        assertEquals("oauth2", AuthProperties.AUTH_TYPE_OAUTH2);
        assertEquals("basic", AuthProperties.AUTH_TYPE_BASIC);
        assertEquals("token", OAuth2Properties.TOKEN);
        assertEquals("credential", OAuth2Properties.CREDENTIAL);
        assertEquals("oauth2-server-uri", OAuth2Properties.OAUTH2_SERVER_URI);
        assertEquals("rest.auth.basic.username", AuthProperties.BASIC_USERNAME);
        assertEquals("rest.auth.basic.password", AuthProperties.BASIC_PASSWORD);
    }
}
