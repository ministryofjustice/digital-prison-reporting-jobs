package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainServiceException;

import java.util.regex.PatternSyntaxException;

@Singleton
public class DomainService {

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    private final DomainDefinitionClient dynamoDB;
    private final DomainExecutor executor;
    private final JobArguments parameters;

    @Inject
    public DomainService(JobArguments parameters,
                         DomainDefinitionClient dynamoDB,
                         DomainExecutor executor) {
        this.parameters = parameters;
        this.dynamoDB = dynamoDB;
        this.executor = executor;
    }

    public void run() throws DomainServiceException, DomainExecutorException {
        runInternal(
                parameters.getDomainRegistry(),
                parameters.getDomainTableName(),
                parameters.getDomainName(),
                parameters.getDomainOperation()
        );
    }

    private void runInternal(
            String domainRegistry,
            String domainTableName,
            String domainName,
            String domainOperation
    ) throws PatternSyntaxException, DomainServiceException, DomainExecutorException {
        if (domainOperation.equalsIgnoreCase("delete"))
            executor.doDomainDelete(domainName, domainTableName);
        else {
            val domain = getDomainDefinition(domainRegistry, domainName, domainTableName);
            processDomain(domain, domain.getName(), domainTableName, domainOperation);
        }
    }

    private void processDomain(
            DomainDefinition domain,
            String domainName,
            String domainTableName,
            String domainOperation
    ) {
        val prefix = "processing of domain: '" + domainName + "' operation: " + domainOperation + " ";

        try {
            logger.info(prefix + "started");
            executor.doFullDomainRefresh(domain, domainTableName, domainOperation);
            logger.info(prefix + "completed");
        } catch (Exception e) {
            logger.error(prefix + "failed", e);
        }
    }

    private DomainDefinition getDomainDefinition(String domainRegistry,
                                                 String domainName,
                                                 String tableName)
            throws DomainServiceException {
        try {
            val response = dynamoDB.executeQuery(domainRegistry, domainName);
            val domainDefinition = dynamoDB.parse(response, tableName);
            logger.info("Retrieved domain definition for '{}'", domainName);
            return domainDefinition;
        } catch (DatabaseClientException e) {
            logger.error("DynamoDB request failed: " + e.getMessage());
            throw new DomainServiceException("DynamoDB request failed: ", e);
        }
    }
}
