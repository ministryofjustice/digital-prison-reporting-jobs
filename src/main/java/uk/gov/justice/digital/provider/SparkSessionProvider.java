package uk.gov.justice.digital.provider;

import com.amazonaws.services.glue.GlueContext;
import jakarta.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;

import java.time.ZoneOffset;
import java.util.TimeZone;

@Singleton
public class SparkSessionProvider {

    public GlueContext createGlueContext(String jobName, JobArguments arguments, JobProperties properties) {
        SparkConf sparkConf = new SparkConf().setAppName(jobName);
        SparkSession sparkSession = getConfiguredSparkSession(sparkConf, arguments, properties);
        return new GlueContext(sparkSession.sparkContext());
    }

    public SparkSession getConfiguredSparkSession(SparkConf sparkConf, JobArguments arguments, JobProperties properties) {

        configureSparkConf(sparkConf, arguments, properties);

        return SparkSession.builder()
                                .config(sparkConf)
                                .enableHiveSupport()
                                .getOrCreate();
    }
    public SparkSession getConfiguredSparkSession(JobArguments arguments, JobProperties properties) {
        return getConfiguredSparkSession(new SparkConf(), arguments, properties);
    }

    public static void configureSparkConf(SparkConf sparkConf, JobArguments arguments, JobProperties properties) {
        // We set the overall default timezone to UTC before then configuring the spark session to also use UTC.
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        String driverMemory = getSparkMemory(arguments, properties.getSparkDriverMemory());
        String executorMemory = getSparkMemory(arguments, properties.getSparkExecutorMemory());
        sparkConf
                .set("spark.driver.memory", driverMemory)
                .set("spark.executor.memory", executorMemory)
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.properties.defaults.targetFileSize", "67110000")
                .set("spark.sql.broadcastTimeout", arguments.getBroadcastTimeoutSeconds().toString())
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.legacy.charVarcharAsString", "true")
                .set("spark.sql.files.maxRecordsPerFile", arguments.getSparkSqlMaxRecordsPerFile().toString())
                // Do not fail when a file gets deleted/archived after it has been read and converted into a dataframe
                .set("spark.sql.files.ignoreMissingFiles", "true")
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

    private static String getSparkMemory(JobArguments arguments, String memory) {
        return arguments.adjustSparkMemory() ? getAdjustedWorkerMemory(memory) : memory;
    }

    private static String getAdjustedWorkerMemory(String memory) {
        switch (memory) {
            case "10g":
                return "11g";
            case "20g":
                return "22g";
            case "40g":
                return "44g";
            case "80g":
                return "88g";
            default:
                return memory;
        }
    }

}
