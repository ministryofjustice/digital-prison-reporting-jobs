package uk.gov.justice.digital.service;

import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.repository.DomainRepository;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

public class DomainService {

    protected String sourcePath;
    protected String targetPath;
    protected DomainRepository repo;

    protected DataStorageService storage;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainService.class);

    public DomainService(final String sourcePath,
                         final String targetPath,
                         final DynamoDBClient dynamoDBClient,
                         final DataStorageService storage) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.repo = new DomainRepository(dynamoDBClient);
        this.storage = storage;
    }

    public void run(final String domainRegistry, final String domainTableName, final String domainName,
                    final String domainOperation) throws PatternSyntaxException{
        if (!domainOperation.equalsIgnoreCase("delete")) {
            Set<DomainDefinition> domains = getDomains(domainRegistry, domainName);
            logger.info("Located " + domains.size() + " domains for name '" + domainName + "'");
            for(final DomainDefinition domain : domains) {
                processDomain(domain, domain.getName(), domainTableName, domainOperation);
            }
        } else {
            processDomain(null, domainName, domainTableName, domainTableName);
        }
    }

    protected Set<DomainDefinition> getDomains(final String domainRegistry, final String domainName)
            throws PatternSyntaxException {
        return this.repo.getForName(domainRegistry, domainName);
    }

    protected void processDomain(final DomainDefinition domain, final String domainName, final String domainTableName,
                                 final String domainOperation) {
        try {
            logger.info("processing of domain '" + domainName + "' started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage);
            executor.doFull(domainName, domainTableName, domainOperation);
            logger.info("processing of domain '" + domainName + "' completed");
        } catch(Exception e) {
            logger.error("processing of domain '" + domainName + "' failed");
            handleError(e);
        }
    }

    protected void handleError(final Exception e) {
        logger.error("processing of domain failed", e);
    }

}
