package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DomainService;

/**
 * Job that refreshes domains so that the data in the consumer-facing systems is correctly formatted and up-to-date.
 * It reads domains from DomainRegistry (a DynamoDB table) and does one of the following:
 * 1. Refresh whole domain
 * 2. Refresh one table in a domain
 */
@Singleton
@CommandLine.Command(name = "DomainRefreshJob")
public class DomainRefreshJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DomainRefreshJob.class);

    private final JobArguments arguments;
    private final DomainService domainService;
    private final SparkSessionProvider sparkSessionProvider;

    @Inject
    public DomainRefreshJob(
            JobArguments arguments,
            DomainService domainService,
            SparkSessionProvider sparkSessionProvider
    ) {
        this.arguments = arguments;
        this.domainService = domainService;
        this.sparkSessionProvider = sparkSessionProvider;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DomainRefreshJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            domainService.run(sparkSessionProvider.getConfiguredSparkSession(arguments.getLogLevel()));
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

    }

}
