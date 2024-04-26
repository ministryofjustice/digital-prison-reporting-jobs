package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.HiveTableService;

import javax.inject.Inject;
import java.util.Set;

/**
 * Job that switches the Hive tables from one s3 bucket to another.
 */
@CommandLine.Command(name = "SwitchHiveTableJob")
public class SwitchHiveTableJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SwitchHiveTableJob.class);
    private final ConfigService configService;
    private final HiveTableService hiveTableService;
    private final SparkSessionProvider sparkSessionProvider;
    private final JobArguments jobArguments;

    @Inject
    public SwitchHiveTableJob(
            ConfigService configService,
            HiveTableService hiveTableService,
            SparkSessionProvider sparkSessionProvider,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.hiveTableService = hiveTableService;
        this.sparkSessionProvider = sparkSessionProvider;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(SwitchHiveTableJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("SwitchHiveTableJob running");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(jobArguments);

            ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                    .getConfiguredTables(jobArguments.getConfigKey());

            Set<ImmutablePair<String, String>> failedTables = hiveTableService.switchPrisonsTableDataSource(spark, configuredTables);

            if (!failedTables.isEmpty()) {
                logger.error("Not all schemas were processed");
                System.exit(1);
            }

            logger.info("SwitchHiveTableJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
