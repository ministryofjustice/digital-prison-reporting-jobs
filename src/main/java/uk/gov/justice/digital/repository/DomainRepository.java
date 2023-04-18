package uk.gov.justice.digital.repository;

import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.common.Util;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.service.DeltaLakeService;
import java.util.HashSet;
import java.util.Set;

public class DomainRepository extends Util {



    private final DynamoDBClient dynamoDBClient;

    protected DeltaLakeService service = new DeltaLakeService();

    public DomainRepository(final DynamoDBClient dynamoDBClient) {
        this.dynamoDBClient = dynamoDBClient;
    }


    public Set<DomainDefinition> getForName(final String domainTableName, final String domainId) {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        DomainDefinition domain = dynamoDBClient.getDomainDefinition(domainTableName, domainId);
        domains.add(domain);
        return domains;
    }
}
