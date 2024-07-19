package uk.gov.justice.digital.config;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.nio.file.Paths;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR_RAW;
import static uk.gov.justice.digital.config.JobArguments.*;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;

public class BaseSparkTest {

	protected static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

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
		JobArguments arguments = new JobArguments(ImmutableMap.of(LOG_LEVEL, "info"));
		return spark = sparkSessionProvider.getConfiguredSparkSession(sparkTestConfiguration, arguments);
	}

	@BeforeAll
	public static void createSession() {
		spark = createSparkSession();
    }

	@AfterAll
	public static void stopSession() {
		spark.stop();
	}

	protected static void assertDeltaTableContainsPK(String tablePath, int primaryKey) {
		Dataset<Row> df = spark.read().format("delta").load(tablePath);
		List<Row> result = df
				.select(PRIMARY_KEY_COLUMN)
				.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(primaryKey)))
				.collectAsList();

		assertTrue(result.contains(RowFactory.create(primaryKey)));
	}

	protected static void assertDeltaTableContainsForPK(String tablePath, String data, int primaryKey) {
		Dataset<Row> df = spark.read().format("delta").load(tablePath);
		List<Row> result = df
				.select(DATA_COLUMN)
				.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(primaryKey)))
				.collectAsList();

		assertTrue(result.contains(RowFactory.create(data)));
	}

	protected static void assertViolationsTableContainsPK(String tablePath, int primaryKey) {
		Dataset<Row> df = spark.read().format("delta").load(tablePath);
		Dataset<Row> errorRawAsJson = spark.read().json(df.select(ERROR_RAW).as(Encoders.STRING()));
		List<Row> result = errorRawAsJson
				.select(PRIMARY_KEY_COLUMN)
				.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(primaryKey)))
				.collectAsList();

		assertTrue(result.contains(RowFactory.create(primaryKey)));
	}


	protected static void assertViolationsTableContainsForPK(String tablePath, String data, int primaryKey) {
		Dataset<Row> df = spark.read().format("delta").load(tablePath);
		Dataset<Row> errorRawAsJson = spark.read().json(df.select(ERROR_RAW).as(Encoders.STRING()));
		List<Row> result = errorRawAsJson
				.select(DATA_COLUMN)
				.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(primaryKey)))
				.collectAsList();

		assertTrue(result.contains(RowFactory.create(data)));
	}

	protected static void assertDeltaTableDoesNotContainPK(String tablePath, int primaryKey) {
		try {
			Dataset<Row> df = spark.read().format("delta").load(tablePath);
			List<Row> result = df
					.select(DATA_COLUMN)
					.where(col(PRIMARY_KEY_COLUMN).equalTo(lit(primaryKey)))
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

	protected static void assertStructuredAndCuratedForTableContainForPK(String structuredPath, String curatedPath, String schemaName, String tableName, String data, int primaryKey) {
		String structuredTablePath = Paths.get(structuredPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		String curatedTablePath = Paths.get(curatedPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		assertDeltaTableContainsForPK(structuredTablePath, data, primaryKey);
		assertDeltaTableContainsForPK(curatedTablePath, data, primaryKey);
	}

	protected static void assertStructuredAndCuratedForTableDoNotContainPK(String structuredPath, String curatedPath, String schemaName, String tableName, int primaryKey) {
		String structuredTablePath = Paths.get(structuredPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		String curatedTablePath = Paths.get(curatedPath).resolve(schemaName).resolve(tableName).toAbsolutePath().toString();
		assertDeltaTableDoesNotContainPK(structuredTablePath, primaryKey);
		assertDeltaTableDoesNotContainPK(curatedTablePath, primaryKey);
	}

	protected static void givenRetrySettingsAreConfigured(JobArguments arguments) {
		when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
		when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
		when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
		when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
	}

}
