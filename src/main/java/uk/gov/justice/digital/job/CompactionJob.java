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
 * Job that runs a delta lake compaction on any tables it finds immediately under the provided Hadoop compatible path.
 * A compaction will rewrite small files into larger files that are more efficient for read operations. It will
 * not remove the old files that it has compacted.
 */
@Command(name = "CompactionJob")
public class CompactionJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionJob.class);
    private final MaintenanceService maintenanceService;
    private final SparkSessionProvider sparkSessionProvider;
    private final JobArguments jobArguments;

    @Inject
    public CompactionJob(
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
        PicocliRunner.run(CompactionJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("Compaction running");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(jobArguments.getLogLevel());
            maintenanceService.compactDeltaTables(spark, jobArguments.getMaintenanceTablesRootPath());
            logger.info("Compaction finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
