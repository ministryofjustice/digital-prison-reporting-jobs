package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.Collections;
import java.util.Map;

@Singleton
public class DomainDefinitionClient {

    private static final String indexName = "secondaryId-type-index";
    private static final String sortKeyName = "secondaryId";
    private static final String dataField = "data";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final AmazonDynamoDB client;
    private final String tableName;

    @Inject
    public DomainDefinitionClient(DynamoDBClientProvider dynamoDBClientProvider,
                                  JobArguments jobArguments) {
        this(dynamoDBClientProvider.getClient(), jobArguments.getDomainRegistry());
    }

    public DomainDefinitionClient(AmazonDynamoDB dynamoDB, String tableName) {
        this.client = dynamoDB;
        this.tableName = tableName;
    }

    public DomainDefinition getDomainDefinition(String domainName, String tableName) throws DatabaseClientException {
        val result = makeRequest(domainName);
        return parseResponse(result, tableName);
    }


    private QueryResult makeRequest(String domainName) throws DatabaseClientException {
        // Set up mapping of the partition name with the value
        val attributeValues = Collections.singletonMap(":" + sortKeyName, new AttributeValue().withS(domainName));
        try {
            QueryRequest queryReq = new QueryRequest()
                    .withTableName(tableName)
                    .withIndexName(indexName)
                    .withKeyConditionExpression(sortKeyName + " = :" + sortKeyName)
                    .withExpressionAttributeValues(attributeValues);
            return client.query(queryReq);
        } catch (AmazonDynamoDBException e) {
            throw new DatabaseClientException("DynamoDB client request failed", e);
        }

    }

    // TODO - the table filtering should happen in the service
    private DomainDefinition parseResponse(QueryResult response, String tableName) throws DatabaseClientException {
        DomainDefinition domainDef = null;
        if (response != null) {
            for (Map<String, AttributeValue> items : response.getItems()) {
                try {
                    String data = items.get(dataField).getS();
                    domainDef = mapper.readValue(data, DomainDefinition.class);
                    if (tableName != null)
                        domainDef.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(tableName));
                } catch (JsonProcessingException e) {
                    throw new DatabaseClientException("JSON Processing failed ", e);
                }
            }
        } else {
            throw new DatabaseClientException("Unable to parse the Query Result");
        }
        return domainDef;
    }

}
