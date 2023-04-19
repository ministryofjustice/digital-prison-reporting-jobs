package uk.gov.justice.digital.config;


import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseSparkTest {

	protected static SparkSession spark;

	private static SparkSession createSparkSession() {
		spark = SparkSession.builder()
			.appName("test")
			.config("spark.master", "local")
			.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
			.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
			.config("spark.databricks.delta.schema.autoMerge.enabled", true)
			.config("spark.databricks.delta.optimizeWrite.enabled", true)
			.config("spark.databricks.delta.autoCompact.enabled", true)
			.config("spark.ui.enabled", false)
			.getOrCreate();
		return spark;
	}

	@BeforeAll
	public static void getOrCreateSparkSession() {
		spark = createSparkSession();
		Assertions.assertNotNull(spark);
	}


	@AfterAll
	public static void stopSparkSession() {
		spark.stop();
	}

	protected Dataset<Row> loadParquetDataframe(final String resource, final String filename) throws IOException {
		return createSparkSession().read().parquet(createFileFromResource(resource, filename).toString());
	}

	protected Path createFileFromResource(final String resource, final String filename) throws IOException {
		final InputStream stream = getStream(resource);
		final File f = Paths.get(filename).toFile();
		FileUtils.copyInputStreamToFile(stream, f);
		return Paths.get(f.getAbsolutePath());
	}

	protected static InputStream getStream(final String resource) {
		InputStream stream = System.class.getResourceAsStream(resource);
		if(stream == null) {
			stream = System.class.getResourceAsStream("/src/test/resources" + resource);
			if(stream == null) {
				stream = System.class.getResourceAsStream("/target/test-classes" + resource);
				if(stream == null) {
					Path root = Paths.get(".").normalize().toAbsolutePath();
					stream = System.class.getResourceAsStream(root + "/src/test/resources" + resource);
					if(stream == null) {
						stream = BaseSparkTest.class.getResourceAsStream(resource);
					}
				}
			}
		}
		return stream;
	}

}
