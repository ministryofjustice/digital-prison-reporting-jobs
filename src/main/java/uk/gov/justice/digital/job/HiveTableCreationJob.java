package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.HiveSchemaService;

import javax.inject.Inject;
import java.util.Set;

import static picocli.CommandLine.Command;

/**
 * Job that replaces Hive tables using the schemas available in the schema registry.
 */
@Command(name = "HiveTableCreationJob")
public class HiveTableCreationJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTableCreationJob.class);
    private final ConfigService configService;
    private final HiveSchemaService hiveSchemaService;
    private final JobArguments jobArguments;

    @Inject
    public HiveTableCreationJob(
            ConfigService configService,
            HiveSchemaService hiveSchemaService,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.hiveSchemaService = hiveSchemaService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(HiveTableCreationJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("HiveTableCreationJob running");

            ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                    .getConfiguredTables(jobArguments.getConfigKey());

            Set<ImmutablePair<String, String>> failedTables = hiveSchemaService.replaceTables(configuredTables);

            if (!failedTables.isEmpty()) {
                logger.error("Not all schemas were processed");
                System.exit(1);
            }

            logger.info("HiveTableCreationJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
