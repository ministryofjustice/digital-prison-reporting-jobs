package uk.gov.justice.digital.service;

import com.amazonaws.services.dynamodbv2.model.QueryResult;
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
import uk.gov.justice.digital.exception.DomainServiceException;

import java.util.HashSet;
import java.util.Set;
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

    public void run() throws DomainServiceException {
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
    ) throws PatternSyntaxException, DomainServiceException {
        if (domainOperation.equalsIgnoreCase("delete")) {
            // TODO - instead of passing null private an alternate method/overload
            processDomain(null, domainName, domainTableName, domainOperation);
        } else {
            val domains = getDomains(domainRegistry, domainName, domainTableName);
            logger.info("Located " + domains.size() + " domains for name '" + domainName + "'");
            for (val domain : domains) {
                processDomain(domain, domain.getName(), domainTableName, domainOperation);
            }
        }
    }

    public Set<DomainDefinition> getDomains(String domainRegistry, String domainName, String domainTableName)
            throws PatternSyntaxException, DomainServiceException {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        domains.add(getDomainDefinition(domainRegistry, domainName, domainTableName));
        return domains;
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
            executor.doFullDomainRefresh(domain, domainName, domainTableName, domainOperation);
            logger.info(prefix + "completed");
        } catch (Exception e) {
            logger.error(prefix + "failed", e);
        }
    }

    private DomainDefinition getDomainDefinition(final String domainRegistry,
                                                 final String domainName, final String tableName)
            throws DomainServiceException {
        try {
            QueryResult response = dynamoDB.executeQuery(domainRegistry, domainName);
            return dynamoDB.parse(response, tableName);
        } catch (DatabaseClientException e) {
            logger.error("DynamoDB request failed: " + e.getMessage());
            throw new DomainServiceException("DynamoDB request failed: ", e);
        }
    }
}
