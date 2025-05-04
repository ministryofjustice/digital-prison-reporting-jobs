package uk.gov.justice.digital.job;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.datareconciliation.DataReconciliationService;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;

import javax.inject.Inject;

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
        SparkJobRunner.run("DataReconciliationJob", jobArguments, properties, sparkSessionProvider, logger, this::runJob);
    }

    @VisibleForTesting
    void runJob(SparkSession sparkSession) {
        DataReconciliationResult results = dataReconciliationService.reconcileData(sparkSession);
        String resultSummary = results.summary();
        if (results.isSuccess()) {
            logger.info("Data reconciliation SUCCEEDED:\n\n{}", resultSummary);
        } else {
            logger.error("Data reconciliation FAILED WITH DIFFERENCES:\n\n{}", resultSummary);
            if (jobArguments.shouldReconciliationFailJobIfChecksFail()) {
                System.exit(1);
            }
        }
    }

}
