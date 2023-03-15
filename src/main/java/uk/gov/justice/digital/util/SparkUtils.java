package uk.gov.justice.digital.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    private static SparkSession sparkSession = null;
    private static SparkConf sparkConf = null;

    public static SparkSession getSparkSession(SparkConf sparkConf) {
        if (sparkSession == null) {
            sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                    .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                    .set("spark.databricks.delta.autoCompact.enabled", "true")
                    .set("spark.sql.legacy.charVarcharAsString", "true");

            sparkSession = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();

        }
        return sparkSession;
    }

    public static SparkConf getSparkConf(String jobName) {
        if (sparkConf == null) {
            sparkConf = new SparkConf().setAppName(jobName);
        }
        return sparkConf;
    }

}