package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.HiveSchemaService;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

import static picocli.CommandLine.Command;

/**
 * Job that replaces Hive tables using the schemas available in the schema registry.
 */
@Command(name = "HiveTableCreationJob")
public class HiveTableCreationJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTableCreationJob.class);
    private final HiveSchemaService hiveSchemaService;

    @Inject
    public HiveTableCreationJob(HiveSchemaService hiveSchemaService) {
        this.hiveSchemaService = hiveSchemaService;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(HiveTableCreationJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("HiveTableCreationJob running");

            // TODO (DPR2-250): Temporarily using a static list of schemas to create Hive tables.
            // This will be replaced with a config possibly loaded from S3 which contains a group of schemas
            Set<String> schemaNames = new HashSet<>();
            schemaNames.add("nomis/offender_external_movements");
            schemaNames.add("nomis/offenders");
            schemaNames.add("nomis/offender_bookings");
            schemaNames.add("nomis/agency_locations");
            schemaNames.add("nomis/agency_internal_locations");
            schemaNames.add("nomis/movement_reasons");

            Set<String> failedTables = hiveSchemaService.replaceTables(schemaNames);

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
