package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.exception.DataReconciliationFailureException;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.datareconciliation.DataReconciliationService;

import javax.inject.Inject;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

/**
 * Job that reconciles.
 */
@CommandLine.Command(name = "DataReconciliationJob")
public class DataReconciliationJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataReconciliationJob.class);

    private final JobProperties properties;
    private final JobArguments jobArguments;
    private final SparkSessionProvider sparkSessionProvider;
    private final DataReconciliationService dataReconciliationService;

    @Inject
    public DataReconciliationJob(
            JobProperties properties,
            JobArguments jobArguments,
            SparkSessionProvider sparkSessionProvider,
            DataReconciliationService dataReconciliationService
    ) {
        this.properties = properties;
        this.jobArguments = jobArguments;
        this.sparkSessionProvider = sparkSessionProvider;
        this.dataReconciliationService = dataReconciliationService;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(DataReconciliationJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        logger.info("Running DataReconciliationJob");
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            if (runLocal) {
                logger.info("Running locally");
                SparkConf sparkConf = new SparkConf().setAppName("DataReconciliationJob local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, jobArguments);
                runJob(spark);
            } else {
                logger.info("Running in Glue");
                String jobName = properties.getSparkJobName();
                val glueContext = sparkSessionProvider.createGlueContext(jobName, jobArguments);
                Job.init(jobName, glueContext, jobArguments.getConfig());
                SparkSession spark = glueContext.getSparkSession();
                runJob(spark);
                Job.commit();
            }
        } catch (DataReconciliationFailureException e) {
            logger.error("Data reconciliation failed: {}", e.getResults().summary());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

        logger.info("DataReconciliationJob completed successfully");
    }

    private void runJob(SparkSession sparkSession) throws RuntimeException {
        dataReconciliationService.reconcileDataOrThrow(sparkSession);
    }

}
