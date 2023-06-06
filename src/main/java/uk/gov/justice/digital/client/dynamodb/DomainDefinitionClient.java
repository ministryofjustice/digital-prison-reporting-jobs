package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;
import java.util.List;

@Singleton
public class DomainDefinitionClient extends DynamoDBClient {

    private static final String primaryKey = "primaryId";
    private static final String indexName = "secondaryId-type-index";
    private static final String sortKeyName = "secondaryId";
    private static final String dataField = "data";



    @Inject
    public DomainDefinitionClient(DynamoDBClientProvider dynamoDBClientProvider,
                                  JobArguments jobArguments) {
        this(dynamoDBClientProvider.getClient(), jobArguments.getDomainRegistry());
    }

    public DomainDefinitionClient(AmazonDynamoDB dynamoDB, String tableName) {
        super(dynamoDB, tableName, primaryKey, indexName, sortKeyName, dataField);
    }

    public DomainDefinition getDomainDefinition(String domainName, String tableName) throws DatabaseClientException {
        val result = makeRequestForAttribute(":" + sortKeyName, domainName);
        return filterDomainToTable(result, tableName);
    }

    // TODO - the table filtering should happen in the service
    private DomainDefinition filterDomainToTable(QueryResult response, String tableName) throws DatabaseClientException {
        List<DomainDefinition> domains = this.parseResponse(response, DomainDefinition.class);
        if(domains != null && !domains.isEmpty()) {
            final DomainDefinition domainDef = domains.get(0);
            domainDef.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(tableName));
            return domainDef;
        }
        return null;
    }

}
