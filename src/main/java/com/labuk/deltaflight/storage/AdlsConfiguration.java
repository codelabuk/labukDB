package com.labuk.deltaflight.storage;

import org.apache.hadoop.conf.Configuration;

/**
 * Builds the Hadoop {@link Configuration} an {@code abfss://} location needs
 * to connect to Azure Data Lake Storage Gen2.
 *
 * <b>The ADLS Gen2-specific parts:</b>
 * <ul>
 *   <li>{@code fs.azure.account.auth.type} — authentication type. Most
 *       common options:
 *       <ul>
 *         <li>{@code OAuth} — service principal or managed identity, the
 *             recommended production approach.</li>
 *         <li>{@code SharedKey} — storage account key, simpler but less
 *             secure. Use for local dev/testing.</li>
 *       </ul>
 *   </li>
 *   <li>{@code fs.azure.account.key.&lt;account&gt;.dfs.core.windows.net} —
 *       the storage account key (SharedKey auth only).</li>
 *   <li>For OAuth/service principal, three more settings:
 *       {@code fs.azure.account.oauth2.client.id},
 *       {@code fs.azure.account.oauth2.client.secret},
 *       {@code fs.azure.account.oauth2.client.endpoint}.</li>
 * </ul>
 *
 * <b>URI format:</b> {@code abfss://container@account.dfs.core.windows.net/path}
 * — note the {@code s} (secure, TLS-only). {@code abfs://} (without the s)
 * is plain HTTP and not recommended even for dev.
 *
 * <b>Requires {@code hadoop-azure} on the classpath</b> — declared as an
 * optional dependency in this project's pom, so it IS present in this
 * project's own build, but a downstream consumer would need to add it
 * explicitly.
 *
 * <b>Not verified against a real ADLS instance</b> — these are the
 * standard, documented Hadoop Azure settings. A real first run is the
 * actual test.
 */
public final class AdlsConfiguration {

    private AdlsConfiguration() {
    }

    /**
     * SharedKey authentication — storage account name + key.
     * Simplest to set up; fine for local dev or testing.
     *
     * @param storageAccount e.g. {@code myaccount}
     * @param accountKey     the base64-encoded storage account key
     */
    public static Configuration forSharedKey(String storageAccount, String accountKey) {
        Configuration conf = new Configuration();
        conf.set("fs.azure.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
        conf.set("fs.AbstractFileSystem.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.Abfss");
        conf.set("fs.azure.account.auth.type." + storageAccount + ".dfs.core.windows.net",
                "SharedKey");
        conf.set("fs.azure.account.key." + storageAccount + ".dfs.core.windows.net",
                accountKey);
        return conf;
    }

    /**
     * OAuth service principal authentication — recommended for production.
     *
     * @param storageAccount e.g. {@code myaccount}
     * @param tenantId       Azure AD tenant ID
     * @param clientId       service principal (app) client ID
     * @param clientSecret   service principal client secret
     */
    public static Configuration forServicePrincipal(String storageAccount, String tenantId,
                                                      String clientId, String clientSecret) {
        Configuration conf = new Configuration();
        conf.set("fs.azure.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
        conf.set("fs.AbstractFileSystem.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.Abfss");
        conf.set("fs.azure.account.auth.type." + storageAccount + ".dfs.core.windows.net",
                "OAuth");
        conf.set("fs.azure.account.oauth.provider.type." + storageAccount + ".dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccount + ".dfs.core.windows.net",
                "https://login.microsoftonline.com/" + tenantId + "/oauth2/token");
        conf.set("fs.azure.account.oauth2.client.id." + storageAccount + ".dfs.core.windows.net",
                clientId);
        conf.set("fs.azure.account.oauth2.client.secret." + storageAccount + ".dfs.core.windows.net",
                clientSecret);
        return conf;
    }

    /**
     * Reads the same settings from environment variables — the shape that
     * fits running the Flight server as a service (Docker, k8s) where
     * credentials come from the environment, not code.
     *
     * Reads: {@code ADLS_STORAGE_ACCOUNT}, {@code ADLS_AUTH_TYPE}
     * ({@code SharedKey} or {@code OAuth}), then either:
     * <ul>
     *   <li>SharedKey: {@code ADLS_ACCOUNT_KEY}</li>
     *   <li>OAuth: {@code ADLS_TENANT_ID}, {@code ADLS_CLIENT_ID},
     *       {@code ADLS_CLIENT_SECRET}</li>
     * </ul>
     * Returns a plain default Configuration if {@code ADLS_STORAGE_ACCOUNT}
     * isn't set, so a deployment not using ADLS doesn't need any of these.
     */
    public static Configuration fromEnvironment() {
        String account = System.getenv("ADLS_STORAGE_ACCOUNT");
        if (account == null || account.isBlank()) {
            return new Configuration();
        }
        String authType = System.getenv("ADLS_AUTH_TYPE");
        if ("OAuth".equalsIgnoreCase(authType)) {
            return forServicePrincipal(account,
                    System.getenv("ADLS_TENANT_ID"),
                    System.getenv("ADLS_CLIENT_ID"),
                    System.getenv("ADLS_CLIENT_SECRET"));
        }
        return forSharedKey(account, System.getenv("ADLS_ACCOUNT_KEY"));
    }
}
