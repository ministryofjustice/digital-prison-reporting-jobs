package uk.gov.justice.digital.provider;

import io.micronaut.logging.LogLevel;
import jakarta.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

@Singleton
public class SparkSessionProvider {

    public SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        SparkSession session = SparkSession.builder()
                                .config(sparkConf)
                                .enableHiveSupport()
                                .getOrCreate();

        session.sparkContext().setLogLevel(LogLevel.INFO.name());

        return session;
    }

    public SparkSession getConfiguredSparkSession() {
        return getConfiguredSparkSession(new SparkConf());
    }

}
