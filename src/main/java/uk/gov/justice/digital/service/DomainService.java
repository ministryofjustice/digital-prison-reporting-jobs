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

    private final DomainDefinitionClient domainClient;
    private final DomainExecutor executor;
    private final JobArguments arguments;

    @Inject
    public DomainService(JobArguments arguments,
                         DomainDefinitionClient domainClient,
                         DomainExecutor executor) {
        this.arguments = arguments;
        this.domainClient = domainClient;
        this.executor = executor;
    }

    public void run() throws DomainServiceException, DomainExecutorException {
        runInternal(
                arguments.getDomainTableName(),
                arguments.getDomainName(),
                arguments.getDomainOperation()
        );
    }

    private void runInternal(
            String domainTableName,
            String domainName,
            String domainOperation
    ) throws PatternSyntaxException, DomainServiceException, DomainExecutorException {
        if (domainOperation.equalsIgnoreCase("delete"))
            executor.doDomainDelete(domainName, domainTableName);
        else {
            val domain = getDomainDefinition(domainName, domainTableName);
            processDomain(domain, domain.getName(), domainTableName, domainOperation);
        }
    }

    private void processDomain(
            DomainDefinition domain,
            String domainName,
            String domainTableName,
            String domainOperation
    ) throws DomainExecutorException {
        val prefix = "processing of domain: '" + domainName + "' operation: " + domainOperation + " ";

        logger.info(prefix + "started");
        executor.doFullDomainRefresh(domain, domainTableName, domainOperation);
        logger.info(prefix + "completed");
    }

    private DomainDefinition getDomainDefinition(String domainName,
                                                 String tableName) throws DomainServiceException {
        try {
            val domainDefinition = domainClient.getDomainDefinition(domainName, tableName);
            logger.info("Retrieved domain definition for domain: '{}' table: '{}'", domainName, tableName);
            return domainDefinition;
        } catch (DatabaseClientException e) {
            logger.error("DynamoDB request failed: ", e);
            throw new DomainServiceException("DynamoDB request failed: ", e);
        }
    }
}
