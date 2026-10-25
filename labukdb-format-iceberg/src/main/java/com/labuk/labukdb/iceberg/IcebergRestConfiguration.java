package com.labuk.labukdb.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the property map an Iceberg REST catalog connection needs, and
 * connects one — the REST-catalog counterpart of {@code MinioConfiguration}/
 * {@code AdlsConfiguration}, same {@code fromEnvironment()} shape.
 *
 * <b>v1 limitation, stated plainly:</b> one REST catalog connection per
 * process, configured from the environment — not a registry of multiple
 * catalogs. Same scope MinIO/ADLS connector config already has here.
 *
 * <b>Zero new dependency.</b> {@code RESTCatalog} and the HTTP client it
 * needs ({@code httpclient5}) are already bundled/transitively resolved
 * via {@code iceberg-core} — confirmed via {@code unzip -l}/{@code mvn
 * dependency:tree} before writing this, not assumed.
 *
 * Property keys are the real ones from {@code iceberg-core}'s own
 * {@link CatalogProperties}/{@link OAuth2Properties}/{@link AuthProperties}
 * (confirmed via {@code javap -constants}, not guessed): {@code uri},
 * {@code warehouse}, {@code rest.auth.type} ({@code none}/{@code basic}/
 * {@code oauth2}/{@code sigv4}/{@code google}), {@code token},
 * {@code credential}, {@code oauth2-server-uri},
 * {@code rest.auth.basic.username}/{@code rest.auth.basic.password}.
 *
 * <b>Implemented, not live-verified</b> — no real or fake REST catalog
 * server is available in this project's development environment. The
 * format-agnostic {@code Catalog}-based loading this feeds into ({@link
 * IcebergTableFormat}/{@link IcebergTableWriter}'s {@code Catalog}
 * constructors) is fully tested against a real {@code InMemoryCatalog};
 * this class's own property-mapping is unit-tested; the actual network
 * round-trip against a real REST server is not — same honesty standard
 * already applied to native S3/ADLS in this project ("implemented, not
 * yet verified against a real endpoint").
 */
public final class IcebergRestConfiguration {

    private IcebergRestConfiguration() {
    }

    /**
     * Reads {@code ICEBERG_REST_URI} (required — throws if unset, since
     * this is only ever called once a caller has already decided a REST
     * catalog is wanted), {@code ICEBERG_REST_WAREHOUSE},
     * {@code ICEBERG_REST_AUTH_TYPE} plus the matching credential
     * variables for that auth type.
     */
    public static Map<String, String> fromEnvironment() {
        String uri = System.getenv("ICEBERG_REST_URI");
        if (uri == null || uri.isBlank()) {
            throw new IllegalStateException(
                    "ICEBERG_REST_URI is required to connect to an Iceberg REST catalog (icebergrest:// table reference)");
        }

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);

        String warehouse = System.getenv("ICEBERG_REST_WAREHOUSE");
        if (warehouse != null && !warehouse.isBlank()) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }

        String authType = System.getenv("ICEBERG_REST_AUTH_TYPE");
        if (authType != null && !authType.isBlank()) {
            props.put(AuthProperties.AUTH_TYPE, authType);
            switch (authType) {
                case AuthProperties.AUTH_TYPE_OAUTH2 -> {
                    putIfSet(props, OAuth2Properties.TOKEN, "ICEBERG_REST_TOKEN");
                    putIfSet(props, OAuth2Properties.CREDENTIAL, "ICEBERG_REST_CREDENTIAL");
                    putIfSet(props, OAuth2Properties.OAUTH2_SERVER_URI, "ICEBERG_REST_OAUTH2_SERVER_URI");
                }
                case AuthProperties.AUTH_TYPE_BASIC -> {
                    putIfSet(props, AuthProperties.BASIC_USERNAME, "ICEBERG_REST_BASIC_USERNAME");
                    putIfSet(props, AuthProperties.BASIC_PASSWORD, "ICEBERG_REST_BASIC_PASSWORD");
                }
                default -> {
                    // "none"/"sigv4"/"google" need no extra credential properties
                    // from here -- sigv4/google derive from the ambient AWS/GCP
                    // credential chain, same as this project's own S3-native
                    // fallback already does for Hadoop's S3A connector.
                }
            }
        }

        return props;
    }

    private static void putIfSet(Map<String, String> props, String key, String envVar) {
        String value = System.getenv(envVar);
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }

    /** Connects and initializes a {@link RESTCatalog} from {@link #fromEnvironment()}. */
    public static Catalog connect() {
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("labukdb-rest", fromEnvironment());
        return catalog;
    }
}
