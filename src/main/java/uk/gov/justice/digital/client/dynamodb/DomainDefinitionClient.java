package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.List;

@Singleton
public class DomainDefinitionClient extends DynamoDBClient {

    private static final Logger logger = LoggerFactory.getLogger(DomainDefinitionClient.class);

    private static final String primaryKey = "primaryId";
    private static final String indexName = "secondaryId-type-index";
    private static final String sortKeyName = "secondaryId";
    private static final String dataField = "data";


    @Inject
    public DomainDefinitionClient(DynamoDBClientProvider dynamoDBClientProvider,
                                  JobArguments jobArguments) {
        this(dynamoDBClientProvider.getClient(), jobArguments.getDomainRegistry());
        logger.info("DomainDefinitionClient initialization complete");
    }

    private DomainDefinitionClient(AmazonDynamoDB dynamoDB, String tableName) {
        super(dynamoDB, tableName, primaryKey, indexName, sortKeyName, dataField);
    }

    public DomainDefinition getDomainDefinition(String domainName, String tableName) throws DatabaseClientException {
        val result = makeRequestForAttribute(":" + sortKeyName, domainName);
        return this.parseResponseItems(result.getItems(), DomainDefinition.class)
                .stream()
                .filter(item -> item.getTables().stream().anyMatch(table -> table.getName().equalsIgnoreCase(tableName)))
                .findFirst()
                .orElseThrow(() -> new DatabaseClientException(
                                String.format("Failed to find any domain with name: %s and table: %s", domainName, tableName)
                        )
                );
    }

    public List<DomainDefinition> getDomainDefinitions() throws DatabaseClientException {
        // This uses a table scan which is not efficient for large tables but unlikely to be an issue for the size of the domain registry
        // TODO: (DPR-647) Investigate solutions e.g. creating a DynamoDB index or reading domains from s3
        val result = scanTable();
        List<java.util.Map<String, AttributeValue>> items = result.getItems();
        return this.parseResponseItems(items, DomainDefinition.class);
    }

}
