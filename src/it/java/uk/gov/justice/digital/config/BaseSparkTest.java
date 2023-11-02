package uk.gov.justice.digital.config;

import io.micronaut.logging.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.util.Collections;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.RECORD_SCHEMA;

public class BaseSparkTest {

	protected static final String DATA_LOAD_RECORD_PATH = "src/it/resources/data/dms_record.json";
	protected static final String DATA_CONTROL_RECORD_PATH = "src/it/resources/data/null-data-dms-record.json";

	protected static final String DATA_UPDATE_RECORD_PATH = "src/it/resources/data/dms_update.json";

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
		return spark = sparkSessionProvider.getConfiguredSparkSession(sparkTestConfiguration, LogLevel.INFO);
	}

	@BeforeAll
	public static void createSession() {
		spark = createSparkSession();
	}

	@AfterAll
	public static void stopSession() {
		spark.stop();
	}

	protected static Dataset<Row> getData(final String path) {
		// Convert the input to the format that the converter will receive as input
		return spark
				.read()
				.option("wholetext", "true")
				.text(path)
				.withColumn(
						"jsonData",
						from_json(
								col("value"),
								RECORD_SCHEMA,
								Collections.singletonMap("mode", "PERMISSIVE")
						)
				)
				.select("jsonData.*");
	}

}