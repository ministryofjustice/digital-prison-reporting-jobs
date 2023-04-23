package uk.gov.justice.digital.repository;

import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domains.model.DomainDefinition;


import java.util.HashSet;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

public class DomainRepository {

    private final DynamoDBClient dynamoDBClient;

    public DomainRepository(final DynamoDBClient dynamoDBClient) {
        this.dynamoDBClient = dynamoDBClient;
    }


    public Set<DomainDefinition> getForName(final String domainTableName, final String domainId)
            throws PatternSyntaxException {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        DomainDefinition domain = dynamoDBClient.getDomainDefinition(domainTableName, domainId);
        domains.add(domain);
        return domains;
    }
}
