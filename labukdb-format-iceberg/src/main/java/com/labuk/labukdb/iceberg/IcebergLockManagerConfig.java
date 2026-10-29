package com.labuk.labukdb.iceberg;

import org.apache.hadoop.conf.Configuration;

/**
 * Activates {@link DynamoDbLockManager} on a Hadoop {@link Configuration}
 * before it's handed to {@code new HadoopTables(conf)} — opt-in, same
 * philosophy as Delta's {@code LockProvider}: zero effect unless the
 * caller has actually configured a lock table via
 * {@code ICEBERG_LOCK_DYNAMODB_TABLE}.
 *
 * Confirmed via {@code javap -constants} on {@code HadoopTables}: the
 * property key Iceberg itself reads is {@code lock-impl}, under the
 * {@code iceberg.tables.hadoop.} prefix {@code HadoopTables
 * .LOCK_PROPERTY_PREFIX} strips before handing the map to {@code
 * LockManagers.from}. Setting this one property is the entire
 * integration surface — no change to Iceberg's own commit code needed.
 */
final class IcebergLockManagerConfig {

    private IcebergLockManagerConfig() {
    }

    /** @return whether a real lock was actually engaged (env var was set) */
    static boolean applyIfConfigured(Configuration conf) {
        String table = System.getenv(DynamoDbLockManager.TABLE_NAME_ENV_VAR);
        if (table != null && !table.isBlank()) {
            conf.set("iceberg.tables.hadoop.lock-impl", DynamoDbLockManager.class.getName());
            return true;
        }
        return false;
    }
}
