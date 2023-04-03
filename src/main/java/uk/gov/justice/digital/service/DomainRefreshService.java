package uk.gov.justice.digital.service;

import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domains.DomainExecutor;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.repository.DomainRepository;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;

// TODO Rename it to DomainService??
public class DomainRefreshService {

    protected String domainFilesPath;
    protected String domainRepositoryPath;
    protected String sourcePath;
    protected String targetPath;
    protected DomainRepository repo;
    private DynamoDBClient dynamoDBClient;

    public DomainRefreshService(final SparkSession spark,
                                final String domainFilesPath,
                                final String domainRepositoryPath,
                                final String sourcePath,
                                final String targetPath,
                                final DynamoDBClient dynamoDBClient) {
        this.domainFilesPath = domainFilesPath;
        this.domainRepositoryPath = domainRepositoryPath;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.dynamoDBClient = dynamoDBClient;
        this.repo = new DomainRepository(spark, domainFilesPath, domainRepositoryPath, dynamoDBClient);
    }

    public void run(final String domainTableName, final String domainId) {
        Set<DomainDefinition> domains = getDomains(domainTableName, domainId);
        System.out.println("Located " + domains.size() + " domains for name '" + domainId + "'");
        for(final DomainDefinition domain : domains) {
            processDomain(domain);
        }
    }

    protected Set<DomainDefinition> getDomains(final String domainTableName, final String domainId) {
        return this.repo.getForName(domainTableName, domainId);
    }

    protected void processDomain(final DomainDefinition domain) {
        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull();
            System.out.println("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch(Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
