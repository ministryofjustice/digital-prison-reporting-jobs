package uk.gov.justice.digital.provider;

import com.amazonaws.services.glue.GlueContext;
import jakarta.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;

import java.time.ZoneOffset;
import java.util.TimeZone;

@Singleton
public class SparkSessionProvider {

    public GlueContext createGlueContext(String jobName, JobArguments arguments) {
        SparkConf sparkConf = new SparkConf().setAppName(jobName);
        SparkSessionProvider.configureSparkConf(sparkConf, arguments);
        return new GlueContext(new SparkContext(sparkConf));
    }

    public SparkSession getConfiguredSparkSession(SparkConf sparkConf, JobArguments arguments) {

        configureSparkConf(sparkConf, arguments);

        return SparkSession.builder()
                                .config(sparkConf)
                                .enableHiveSupport()
                                .getOrCreate();
    }
    public SparkSession getConfiguredSparkSession(JobArguments arguments) {
        return getConfiguredSparkSession(new SparkConf(), arguments);
    }

    public static void configureSparkConf(SparkConf sparkConf, JobArguments arguments) {
        // We set the overall default timezone to UTC before then configuring the spark session to also use UTC.
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        sparkConf
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.properties.defaults.targetFileSize", "67110000")
                .set("spark.sql.broadcastTimeout", arguments.getBroadcastTimeoutSeconds().toString())
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

        if (arguments.disableAutoBroadcastJoinThreshold()) {
            sparkConf
                    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                    .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1");
        }
    }

}
