package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.MaintenanceService;

import javax.inject.Inject;

import static picocli.CommandLine.Command;

/**
 * Job that runs a delta lake vacuum on any tables it finds immediately under the provided Hadoop compatible path.
 * A vacuum removes files which have expired in the delta lake history. If run after the compaction job this
 * job will remove the pre-compaction files once their table's retention period `delta.deletedFileRetentionDuration`
 * has passed.
 */
@Command(name = "VacuumJob")
public class VacuumJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(VacuumJob.class);
    private final MaintenanceService maintenanceService;
    private final SparkSessionProvider sparkSessionProvider;
    private final JobArguments jobArguments;

    @Inject
    public VacuumJob(
            MaintenanceService maintenanceService,
            SparkSessionProvider sparkSessionProvider,
            JobArguments jobArguments
    ) {
        this.maintenanceService = maintenanceService;
        this.sparkSessionProvider = sparkSessionProvider;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(VacuumJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("Vacuum running");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(jobArguments.getLogLevel());

            String rootPath = jobArguments.getMaintenanceTablesRootPath();
            int maxDepth = jobArguments.getMaintenanceListTableRecurseMaxDepth();

            maintenanceService.vacuumDeltaTables(spark, rootPath, maxDepth);
            logger.info("Vacuum finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
