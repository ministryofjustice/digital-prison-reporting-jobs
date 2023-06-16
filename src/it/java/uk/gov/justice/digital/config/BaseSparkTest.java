package uk.gov.justice.digital.config;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseSparkTest {

	protected static SparkSession spark;

	private static SparkSession createSparkSession() {
		spark = SparkSession.builder()
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
		return spark;
	}

	@BeforeAll
	public static void createSession() {
		spark = createSparkSession();
    }

	@AfterAll
	public static void stopSession() {
		spark.stop();
	}

}
