package uk.gov.justice.digital.config;

import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.test.SparkSessionExtension;

public class BaseSparkTest {

	protected static SparkSession spark = SparkSessionExtension.getSession();

}
