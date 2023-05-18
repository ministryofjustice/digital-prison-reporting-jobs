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
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.common.ColumnNames.DATA;

@Singleton
public class DomainDefinitionClient {

    private static final String indexName = "secondaryId-type-index";
    private static final String sortKeyName = "secondaryId";

    private final AmazonDynamoDB dynamoDB;
    private final ObjectMapper mapper;


    @Inject
    public DomainDefinitionClient(DynamoDBClientProvider dynamoDBClientProvider) {
        this(dynamoDBClientProvider.getClient(), new ObjectMapper());
    }

    public DomainDefinitionClient(AmazonDynamoDB dynamoDB, ObjectMapper mapper) {
        this.dynamoDB = dynamoDB;
        this.mapper = mapper;
    }


    public DomainDefinition parse(QueryResult response, String tableName) throws DatabaseClientException {
        DomainDefinition domainDef = null;
        if (response != null) {
            for (Map<String, AttributeValue> items : response.getItems()) {
                try {
                    String data = items.get(DATA).getS();
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

    public QueryResult executeQuery(final String domainTableName, final String domainName)
            throws DatabaseClientException {
        // Set up mapping of the partition name with the value
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":" + sortKeyName, new AttributeValue().withS(domainName));
        try {
            QueryRequest queryReq = new QueryRequest()
                    .withTableName(domainTableName)
                    .withIndexName(indexName)
                    .withKeyConditionExpression(sortKeyName + " = :" + sortKeyName)
                    .withExpressionAttributeValues(attrValues);
            return dynamoDB.query(queryReq);
        } catch (AmazonDynamoDBException e) {
            throw new DatabaseClientException("Query execution failed", e);
        }

    }
}
