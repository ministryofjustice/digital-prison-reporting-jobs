package uk.gov.justice.digital.job;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public abstract class Job {
    protected static SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

}
