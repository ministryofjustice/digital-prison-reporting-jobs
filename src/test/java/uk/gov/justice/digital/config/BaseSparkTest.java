package uk.gov.justice.digital.config;



import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.schema_of_json;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class BaseSparkTest {

	@TempDir
	Path folder;
	
	protected SparkSession spark;
	

	protected String accessKey;
	protected String secretKey;

	protected SparkSession createSparkSession() {
		spark = SparkSession.builder()
				.appName("test")
//				.enableHiveSupport()
				.config("spark.master", "local")
				// important delta configurations
				// =================================
				// these need to be in the cloud-platform
				.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
				.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
				.config("spark.databricks.delta.schema.autoMerge.enabled", true)
				.config("spark.databricks.delta.optimizeWrite.enabled", true)
				.config("spark.databricks.delta.autoCompact.enabled", true)
				// ============================
				// these are needed for test but NOT for live
				// the manifest needs a HiveContext and this handles a separate one for each test
				// otherwise we do an inmem one : jdbc:derby://localhost:1527/memory:myInMemDB;create=true
//				.config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" +
//						folder.toFile().getAbsolutePath() + "/metastore_db_test;create=true")
				.getOrCreate();

		try {
			final AWSCredentials credentials = new ProfileCredentialsProvider("moj").getCredentials();
			accessKey = credentials.getAWSAccessKeyId();
			secretKey = credentials.getAWSSecretKey();

			spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true");
			spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl",
					org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
			spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKey);
			spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretKey);
			spark.sparkContext().hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl",
					"org.apache.hadoop.fs.s3a.S3A");
		} catch(Exception e) {
			// lets not do AWS
		}
		return spark;
	}

	@BeforeEach
	public void before() {
		this.spark = createSparkSession();
		Assertions.assertNotNull(this.spark);
	}


	@AfterEach
	public void after() {
		spark.stop();
		// cleanup derby database
	}

	public SparkSession getSparkSession() {
		return this.spark;
	}
	
	// Test
	protected boolean hasField(final Dataset<Row> in, final String fieldname) {
		return Arrays.asList(in.schema().fieldNames()).contains(fieldname);
	}
	
	protected Object getFirstValue(final Dataset<Row> in, final String fieldname) {
		final Row first = in.first();
		return first.get(first.fieldIndex(fieldname));
	}
	
	protected Dataset<Row> getPayload(Dataset<Row> df, final String column, final String parsed) {
		final DataType schema = getSchema(df, column);
		return df.withColumn(parsed, from_json(col(column), schema));
	}

	protected DataType getSchema(Dataset<Row> df, final String column) {
		final Row[] schs = (Row[])df.sqlContext().range(1).select(
				schema_of_json(lit(df.select(column).first().getString(0)))
		).collect();
		final String schemaStr = schs[0].getString(0);

		return DataType.fromDDL(schemaStr);
	}
	
	
	protected Dataset<Row> loadDataframe(final String resource, final String filename) throws IOException {
		return spark.read().load(createFileFromResource(resource, filename).toString());
	}
	
	protected Dataset<Row> loadCsvDataframe(final String resource, final String filename) throws IOException {
		return spark.read().option("header", "true").format("csv").load(createFileFromResource(resource, filename).toString());
	}
	
	protected Dataset<Row> loadJsonDataframe(final String resource, final String filename) throws IOException {
		return spark.read().option("multiline", "true").format("json").load(createFileFromResource(resource, filename).toString());
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

	protected Path createFileFromResource(final String resource, final String filename, final String directory)
			throws IOException {
		final InputStream stream = getStream(resource);
		File dir;
		if(Files.exists(Paths.get(folder.toFile().getAbsolutePath(),directory))) {
			dir = new File(folder.toFile().getAbsolutePath() + "/" + directory);
		} else {
			dir = Paths.get(directory).toFile();
		}
		final File f = new File(dir, filename);
		FileUtils.copyInputStreamToFile(stream, f);
		return Paths.get(f.getAbsolutePath());
	}

	@SuppressWarnings("deprecation")
	protected String getResource(final String resource) throws IOException {
		final InputStream stream = getStream(resource);
		return IOUtils.toString(stream);
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


	protected boolean areEqual(final Dataset<Row> a, final Dataset<Row> b) {
		if(!a.schema().equals(b.schema()))
			return false;
		final List<Row> al = a.collectAsList();
		final List<Row> bl = b.collectAsList();

		if(al == null && bl == null) return true;

		if(al.isEmpty() && bl.isEmpty()) return true;
		if(al.isEmpty() && !bl.isEmpty()) return false;
		if(!al.isEmpty() && bl.isEmpty()) return false;

		return CollectionUtils.subtract(al, bl).size() == 0;
	}
}
