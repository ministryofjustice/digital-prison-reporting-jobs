package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.MaintenanceService;

import javax.inject.Inject;
import java.util.Optional;

import static picocli.CommandLine.Command;

/**
 * Job that runs a delta lake compaction on tables belonging to a provided domain or otherwise any tables it finds immediately under the provided Hadoop compatible path.
 * A compaction will rewrite small files into larger files that are more efficient for read operations. It will
 * not remove the old files that it has compacted until a separate vacuum is run.
 */
@Command(name = "CompactionJob")
public class CompactionJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionJob.class);

    private final MaintenanceService maintenanceService;
    private final ConfigService configService;
    private final SparkSessionProvider sparkSessionProvider;
    private final JobArguments jobArguments;

    @Inject
    public CompactionJob(
            MaintenanceService maintenanceService,
            ConfigService configService,
            SparkSessionProvider sparkSessionProvider,
            JobArguments jobArguments
    ) {
        this.maintenanceService = maintenanceService;
        this.configService = configService;
        this.sparkSessionProvider = sparkSessionProvider;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(CompactionJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("Compaction running");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(jobArguments);

            String rootPath = jobArguments.getMaintenanceTablesRootPath();
            int maxDepth = jobArguments.getMaintenanceListTableRecurseMaxDepth();
            Optional<String> optionalConfigKey = jobArguments.getOptionalConfigKey();

            if (optionalConfigKey.isPresent()) {
                ImmutableSet<String> configuredTablePaths = configService.getConfiguredTablePaths(optionalConfigKey.get());
                maintenanceService.compactDeltaTables(spark, rootPath, configuredTablePaths, maxDepth);
            } else {
                maintenanceService.compactDeltaTables(spark, rootPath, ImmutableSet.of(), maxDepth);
            }

            logger.info("Compaction finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
