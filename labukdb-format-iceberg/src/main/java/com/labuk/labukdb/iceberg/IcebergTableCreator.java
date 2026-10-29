package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;

import java.util.List;
import java.util.Map;

/**
 * Creates a brand-new Iceberg table — the capability this project has
 * never had: every test and fixture up to this point called {@code
 * HadoopTables.create}/{@code Catalog.createTable} directly with a
 * hand-built {@code org.apache.iceberg.Schema}. This is the Iceberg
 * counterpart of {@code core.log.TableCreator}, same two inputs (an
 * Arrow {@link Schema} plus a flat partition-column list) translated via
 * {@link ArrowToIcebergSchema} and an identity-only {@link PartitionSpec}
 * — the same Hive-style "exact value, no transform" partitioning {@code
 * core.log.TableCreator}/{@code CommitWriter} already use, not Iceberg's
 * richer bucket/truncate/time-transform partitioning, which this class
 * deliberately doesn't expose (a separate, later decision, not needed
 * for write-lifecycle parity with Delta).
 *
 * <b>Both of {@link IcebergTableWriter}'s own two constructor shapes get
 * a matching {@code createTable}</b> — path-based ({@link
 * #createTable(TableLocation, Schema, List, Map)}, via {@code
 * HadoopTables}) and catalog-based ({@link #createTable(Catalog,
 * TableIdentifier, Schema, List, Map)}, via the {@code Catalog} interface
 * every real catalog implementation shares) — so a table created either
 * way is immediately usable by {@link IcebergTableWriter}/{@link
 * IcebergTableFormat}'s existing matching constructor, no new wiring
 * needed on the write/read side.
 */
public final class IcebergTableCreator {

    /**
     * @param location         where the new table lives, local or remote
     * @param arrowSchema      the table's physical columns
     * @param partitionColumns in the table's actual partition order; identity transform only
     * @param configuration    arbitrary table properties; {@code null} treated as empty
     * @throws RuntimeException (Iceberg's own {@code AlreadyExistsException}) if a table already exists at {@code location}
     */
    public void createTable(TableLocation location, Schema arrowSchema, List<String> partitionColumns,
                             Map<String, String> configuration) {
        org.apache.iceberg.Schema icebergSchema = ArrowToIcebergSchema.toIcebergSchema(arrowSchema);
        PartitionSpec spec = buildPartitionSpec(icebergSchema, partitionColumns);
        Configuration conf = location.isRemote() ? location.hadoopConf() : new Configuration();
        new HadoopTables(conf).create(icebergSchema, spec, SortOrder.unsorted(),
                configuration == null ? Map.of() : configuration, location.raw());
    }

    /**
     * Same as {@link #createTable(TableLocation, Schema, List, Map)}, but
     * via any {@link Catalog} implementation (REST, Hive Metastore, Glue,
     * {@code InMemoryCatalog}, ...) rather than path-based {@code
     * HadoopTables} — matching {@link IcebergTableWriter#IcebergTableWriter(Catalog, TableIdentifier)}'s
     * own catalog-based constructor. The caller is responsible for the
     * identifier's namespace already existing (same requirement every
     * direct {@code catalog.createTable} call already has — not something
     * this method can create on the caller's behalf, since not every
     * {@code Catalog} even supports namespaces, e.g. plain {@code
     * HadoopCatalog}).
     */
    public void createTable(Catalog catalog, TableIdentifier identifier, Schema arrowSchema,
                             List<String> partitionColumns, Map<String, String> configuration) {
        org.apache.iceberg.Schema icebergSchema = ArrowToIcebergSchema.toIcebergSchema(arrowSchema);
        PartitionSpec spec = buildPartitionSpec(icebergSchema, partitionColumns);
        catalog.createTable(identifier, icebergSchema, spec, configuration == null ? Map.of() : configuration);
    }

    private PartitionSpec buildPartitionSpec(org.apache.iceberg.Schema icebergSchema, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        for (String column : partitionColumns) {
            builder.identity(column);
        }
        return builder.build();
    }
}
