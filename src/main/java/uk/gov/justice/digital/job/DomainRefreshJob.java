package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.AWSGlue;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.config.JobParameters;

import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainService;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.PatternSyntaxException;

/**
 * Job that refreshes domains so that the data in the consumer-facing systems is correctly formatted and up-to-date.
 *  It reads domains from DomainRegistry (a DynamoDB table) and does one of the following:
 *   1. Refresh whole domain
 *   2. Refresh one table in a domain
 */
@Singleton
@CommandLine.Command(name = "DomainRefreshJob")
public class DomainRefreshJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DomainRefreshJob.class);

    private final String curatedPath;
    private final String domainTargetPath;
    protected String domainTableName;
    private final String domainName;
    private final String domainOperation;
    private final DynamoDBClient dynamoDBClient;

    private final String domainRegistry;

    private final DataStorageService storage;
    private final AWSGlue glueClient;

    @Inject
    public DomainRefreshJob(JobParameters jobParameters, DynamoDBClient dynamoDBClient, DataStorageService storage) {
        this.curatedPath = jobParameters.getCuratedS3Path();
        this.domainTargetPath = jobParameters.getDomainTargetPath();
        this.domainTableName = jobParameters.getDomainTableName();
        this.domainName = jobParameters.getDomainName();
        this.domainRegistry = jobParameters.getDomainRegistry();
        this.domainOperation = jobParameters.getDomainOperation();
        this.dynamoDBClient = dynamoDBClient;
        this.storage = storage;
        this.glueClient = jobParameters.getGlueClient();
    }

    public DomainService refresh() {
        return new DomainService(curatedPath, domainTargetPath, dynamoDBClient, storage, glueClient);

    }
    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DomainRefreshJob.class);
    }

    @Override
    public void run() {
        DomainService domainService = refresh();
        try {
            domainService.run(domainRegistry, domainTableName, domainName, domainOperation);
        } catch (PatternSyntaxException e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.getBuffer().toString());
        }

    }

}
