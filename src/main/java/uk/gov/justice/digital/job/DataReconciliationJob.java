package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.datareconciliation.DataReconciliationService;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import javax.inject.Inject;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

/**
 * Job that runs data reconciliation in the DataHub.
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

    public static void main(String... args) {
        PicocliMicronautExecutor.execute(DataReconciliationJob.class, args);
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
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

        logger.info("DataReconciliationJob completed successfully");
    }

    private void runJob(SparkSession sparkSession) {
        DataReconciliationResults results = dataReconciliationService.reconcileData(sparkSession);
        String resultSummary = results.summary();
        if (results.isFailure()) {
            logger.error("Data reconciliation FAILED WITH DIFFERENCES:\n{}", resultSummary);
            System.exit(1);
        } else {
            logger.info("Data reconciliation SUCCEEDED:\n{}", resultSummary);
        }
    }

}
