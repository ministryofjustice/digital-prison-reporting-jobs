package uk.gov.justice.digital.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class SparkSessionExtension implements AfterAllCallback {

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        getSession().stop();
    }

    // Internally the getOrCreate() method is synchronized so successive calls are thread safe and will only return the
    // same session instance. We also only create the session when it's explicitly requested by a test, so running a
    // single non-spark test will not be delayed by unnecessary spark startup.
    public static SparkSession getSession() {
        return SparkSession.builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.cores.max", 1)
                .config("spark.executor.cores", 1)
                .config("spark.default.parallelism", 1)
                .config("spark.sql.shuffle.partitions", 1)
                .config("spark.databricks.delta.snapshotPartitions", 1)
                .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", 1)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.schema.autoMerge.enabled", true)
                .config("spark.databricks.delta.optimizeWrite.enabled", true)
                .config("spark.databricks.delta.autoCompact.enabled", true)
                .config("spark.ui.enabled", false)
                // Use an in-memory derby database to allow parallel spark tests to access their own derby database
                // rather than attempting to access one on disk which may already be in use causing failures.
                .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
                .enableHiveSupport()
                .getOrCreate();
    }

}
