package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.util.function.Consumer;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

public class SparkJobRunner {

    public static void run(
            String jobLabel,
            JobArguments jobArguments,
            JobProperties jobProperties,
            SparkSessionProvider sparkSessionProvider,
            Logger logger,
            Consumer<SparkSession> runJob
    ) {
        logger.info("Running {}", jobLabel);
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            if (runLocal) {
                logger.info("Running {} locally", jobLabel);
                SparkConf sparkConf = new SparkConf().setAppName(jobLabel + " local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, jobArguments, jobProperties);
                runJob.accept(spark);
            } else {
                logger.info("Running {} in Glue", jobLabel);
                String jobName = jobProperties.getSparkJobName();
                val glueContext = sparkSessionProvider.createGlueContext(jobName, jobArguments, jobProperties);
                Job.init(jobName, glueContext, jobArguments.getConfig());
                SparkSession spark = glueContext.getSparkSession();
                runJob.accept(spark);
                Job.commit();
            }
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

        logger.info("{} completed successfully", jobLabel);
    }

    private SparkJobRunner() {}
}
