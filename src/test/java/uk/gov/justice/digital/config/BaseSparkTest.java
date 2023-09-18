package uk.gov.justice.digital.config;

import io.micronaut.logging.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import uk.gov.justice.digital.provider.SparkSessionProvider;

public class BaseSparkTest {

	private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

	private static final SparkConf sparkTestConfiguration = new SparkConf()
			.setAppName("test")
			.set("spark.master", "local")
			// Limit parallelism so tests run more quickly.
			.set("spark.cores.max", "1")
			.set("spark.ui.enabled", "false")
			.set("spark.executor.cores", "1")
			.set("spark.default.parallelism", "1")
			.set("spark.sql.shuffle.partitions", "1")
			.set("spark.databricks.delta.snapshotPartitions", "1")
			.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "1")
			// Use an in-memory derby database to allow parallel spark tests to access their own derby database
			// rather than attempting to access one on disk which may already be in use causing failures.
			.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true");

	protected static SparkSession spark;

	private static SparkSession createSparkSession() {
		return spark = sparkSessionProvider.getConfiguredSparkSession(sparkTestConfiguration, LogLevel.INFO, false);
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
