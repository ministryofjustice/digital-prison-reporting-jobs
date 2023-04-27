package uk.gov.justice.digital.repository;

import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

@Singleton
public class DomainRepository {

    private final DynamoDBClient dynamoDBClient;

    @Inject
    public DomainRepository(final DynamoDBClient dynamoDBClient) {
        this.dynamoDBClient = dynamoDBClient;

    }

    public Set<DomainDefinition> getForName(final String domainRegistry, final String domainId)
            throws PatternSyntaxException {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        DomainDefinition domain = dynamoDBClient.getDomainDefinition(domainRegistry, domainId);
        domains.add(domain);
        return domains;
    }
}
