package uk.gov.justice.digital.provider;

import com.amazonaws.services.glue.GlueContext;
import io.micronaut.logging.LogLevel;
import jakarta.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.ZoneOffset;
import java.util.TimeZone;

@Singleton
public class SparkSessionProvider {

    public GlueContext createGlueContext(String jobName, LogLevel logLevel) {
        SparkConf sparkConf = new SparkConf().setAppName(jobName);
        SparkSessionProvider.configureSparkConf(sparkConf);
        SparkContext spark = new SparkContext(sparkConf);
        spark.setLogLevel(logLevel.name().toUpperCase());
        GlueContext glueContext = new GlueContext(spark);
        return glueContext;
    }

    public SparkSession getConfiguredSparkSession(SparkConf sparkConf, LogLevel logLevel) {

        configureSparkConf(sparkConf);

        SparkSession session = SparkSession.builder()
                                .config(sparkConf)
                                .enableHiveSupport()
                                .getOrCreate();

        session.sparkContext().setLogLevel(logLevel.name());

        return session;
    }
    public SparkSession getConfiguredSparkSession(LogLevel logLevel) {
        return getConfiguredSparkSession(new SparkConf(), logLevel);
    }

    public static void configureSparkConf(SparkConf sparkConf) {
        // We set the overall default timezone to UTC before then configuring the spark session to also use UTC.
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        sparkConf
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.properties.defaults.targetFileSize", "67110000")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.legacy.charVarcharAsString", "true")
                // We can write dates as is since we will always be using Spark 3+. See SQLConf for context.
                .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
                .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
                .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
                // Standardise on UTC.
                .set("spark.sql.session.timeZone", "UTC");
    }

}
