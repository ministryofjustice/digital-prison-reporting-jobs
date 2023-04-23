package uk.gov.justice.digital.service;

import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domains.DomainExecutor;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.repository.DomainRepository;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

public class DomainService {

    protected String sourcePath;
    protected String targetPath;
    protected DomainRepository repo;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainService.class);

    public DomainService(final String sourcePath,
                         final String targetPath,
                         final DynamoDBClient dynamoDBClient) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.repo = new DomainRepository(dynamoDBClient);
    }

    public void run(final String domainTableName, final String domainId, final String domainOperation)
            throws PatternSyntaxException{
        Set<DomainDefinition> domains = getDomains(domainTableName, domainId);
        logger.info("Located " + domains.size() + " domains for name '" + domainId + "'");
        for(final DomainDefinition domain : domains) {
            processDomain(domain, domainOperation);
        }
    }

    protected Set<DomainDefinition> getDomains(final String domainTableName, final String domainId)
            throws PatternSyntaxException {
        return this.repo.getForName(domainTableName, domainId);
    }

    protected void processDomain(final DomainDefinition domain, final String domainOperation) {
        try {
            logger.info("DomainService::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            logger.info("DomainService::process('" + domain.getName() + "') completed");
        } catch(Exception e) {
            logger.info("DomainService::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.getBuffer().toString());
    }

}
