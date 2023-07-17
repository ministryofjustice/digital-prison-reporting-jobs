package uk.gov.justice.digital.provider;

import io.micronaut.logging.LogLevel;
import jakarta.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.time.ZoneOffset;
import java.util.TimeZone;

@Singleton
public class SparkSessionProvider {

    public SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        // We set the overall default timezone to UTC before then configuring the spark session to also use UTC.
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));

        sparkConf
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.legacy.charVarcharAsString", "true")
                // We can write dates as is since we will always be using Spark 3+. See SQLConf for context.
                .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
                // Standardise on UTC.
                .set("spark.sql.session.timeZone", "UTC");

        SparkSession session = SparkSession.builder()
                                .config(sparkConf)
                                .enableHiveSupport()
                                .getOrCreate();

        session.sparkContext().setLogLevel(LogLevel.WARN.name());

        return session;
    }

    public SparkSession getConfiguredSparkSession() {
        return getConfiguredSparkSession(new SparkConf());
    }

}
