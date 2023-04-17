package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.repository.DomainRepository;
import uk.gov.justice.digital.service.DomainRefreshService;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Job that refreshes domains so that the data in the consumer-facing systems is correctly formatted and up-to-date.
 *  It reads domains from DomainRegistry (a DynamoDB table) and does one of the following:
 *   1. Refresh whole domain
 *   2. Refresh one table in a domain
 */
@Singleton
@CommandLine.Command(name = "DomainRefreshJob")
public class DomainRefreshJob extends Job implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DomainRefreshJob.class);

    private String domainFilesPath;
    private String domainRepoPath;
    private String curatedPath;
    private String domainTargetPath;
    protected String domainTableName;
    private String domainId;
    private String domainOperation;
    private DynamoDBClient dynamoDBClient;

    @Inject
    public DomainRefreshJob(JobParameters jobParameters, DynamoDBClient dynamoDBClient) {
        this.curatedPath = jobParameters.getCuratedS3Path()
                .orElseThrow(() -> new IllegalStateException(
                        "curated s3 path not set - unable to create CuratedZone instance"
                ));
        this.domainTargetPath = jobParameters.getDomainTargetPath();
        this.domainTableName = jobParameters.getDomainTableName();
        this.domainId = jobParameters.getDomainId();
        this.domainOperation = jobParameters.getDomainOperation();
        this.dynamoDBClient = dynamoDBClient;
    }

    public DomainRefreshService refresh() {
        SparkSession spark = getConfiguredSparkSession(new SparkConf());
        return new DomainRefreshService(spark, curatedPath, domainTargetPath, dynamoDBClient);
    }
    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DomainRefreshJob.class);
    }

    @Override
    public void run() {
        DomainRefreshService domainRefreshService = refresh();
        domainRefreshService.run(domainTableName, domainId, domainOperation);
    }

}
