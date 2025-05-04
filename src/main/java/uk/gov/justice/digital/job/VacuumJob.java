package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.MaintenanceService;

import javax.inject.Inject;
import java.util.Optional;

import static java.lang.String.format;
import static picocli.CommandLine.Command;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;

/**
 * Job that runs a delta lake vacuum on tables belonging to a provided domain or otherwise any tables it finds immediately under the provided Hadoop compatible path.
 * A vacuum removes files which have expired in the delta lake history. If run after the compaction job this
 * job will remove the pre-compaction files once their table's retention period `delta.deletedFileRetentionDuration`
 * has passed.
 */
@Command(name = "VacuumJob")
public class VacuumJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(VacuumJob.class);
    private final MaintenanceService maintenanceService;
    private final ConfigService configService;
    private final SparkSessionProvider sparkSessionProvider;
    private final JobArguments jobArguments;
    private final JobProperties properties;

    @Inject
    public VacuumJob(
            MaintenanceService maintenanceService,
            ConfigService configService,
            SparkSessionProvider sparkSessionProvider,
            JobArguments jobArguments,
            JobProperties properties
    ) {
        this.maintenanceService = maintenanceService;
        this.configService = configService;
        this.sparkSessionProvider = sparkSessionProvider;
        this.jobArguments = jobArguments;
        this.properties = properties;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(VacuumJob.class, args);
    }

    @Override
    public void run() {
        SparkJobRunner.run("VacuumJob", jobArguments, properties, sparkSessionProvider, logger, this::runJob);
    }

    private void runJob(SparkSession sparkSession) {
        String rootPath = jobArguments.getMaintenanceTablesRootPath();
        int maxDepth = jobArguments.getMaintenanceListTableRecurseMaxDepth();
        Optional<String> optionalConfigKey = jobArguments.getOptionalConfigKey();

        if (optionalConfigKey.isPresent()) {
            ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(optionalConfigKey.get());
            configuredTables.forEach(table -> maintenanceService.vacuumDeltaTables(sparkSession, format("%s%s", ensureEndsWithSlash(rootPath), table.right), 0));
        } else {
            maintenanceService.vacuumDeltaTables(sparkSession, ensureEndsWithSlash(rootPath), maxDepth);
        }
    }
}
