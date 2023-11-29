package uk.gov.justice.digital.config;

import io.micronaut.logging.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.RECORD_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;

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

	public static void assertDeltaTableContainsForPK(String tablePath, String data, int primaryKey) {
		Dataset<Row> df = spark.read().format("delta").load(tablePath);
		List<Row> result = df
				.select(DATA_COLUMN)
				.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
				.collectAsList();

		List<Row> expected = Collections.singletonList(RowFactory.create(data));
		assertEquals(expected.size(), result.size());
		assertTrue(result.containsAll(expected));
	}

	public static void assertDeltaTableDoesNotContainPK(String tablePath, int primaryKey) {
		try {
			Dataset<Row> df = spark.read().format("delta").load(tablePath);
			List<Row> result = df
					.select(DATA_COLUMN)
					.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
					.collectAsList();
			assertEquals(0, result.size());
		} catch (Exception e) {
			// If the path doesn't exist then the delta table definitely does not contain the primary key
			// so we squash the Exception in that case. We can't be more specific than Exception in what we
			// catch because the Java compiler will complain that AnalysisException isn't declared as thrown
			// due to Scala trickery.
			if(!e.getMessage().startsWith("Path does not exist")) {
				throw e;
			}
		}
	}

	public static void assertStructuredAndCuratedForTableContainForPK(String structuredPath, String curatedPath, String schemaName, String tableName, String data, int primaryKey) {
		String structuredTablePath = Paths.get(structuredPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		String curatedTablePath = Paths.get(curatedPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		assertDeltaTableContainsForPK(structuredTablePath, data, primaryKey);
		assertDeltaTableContainsForPK(curatedTablePath, data, primaryKey);
	}

	public static void assertStructuredAndCuratedForTableDoNotContainPK(String structuredPath, String curatedPath, String schemaName, String tableName, int primaryKey) {
		String structuredTablePath = Paths.get(structuredPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		String curatedTablePath = Paths.get(curatedPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		assertDeltaTableDoesNotContainPK(structuredTablePath, primaryKey);
		assertDeltaTableDoesNotContainPK(curatedTablePath, primaryKey);
	}

	public static void givenRetrySettingsAreConfigured(JobArguments arguments) {
		when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
		when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
		when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
		when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
	}

}