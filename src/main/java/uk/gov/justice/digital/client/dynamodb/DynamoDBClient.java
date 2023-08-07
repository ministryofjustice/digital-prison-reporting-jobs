package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.*;

public abstract class DynamoDBClient {

    private final AmazonDynamoDB client;
    private final String tableName;

    private final String primaryKey;

    private final String indexName;

    protected final String sortKeyName;

    private final String dataField;
    protected static final ObjectMapper mapper = new ObjectMapper();

    protected DynamoDBClient(AmazonDynamoDB dynamoDB, String tableName, String primaryKey) {
        this(dynamoDB, tableName, primaryKey, null, null, null);
    }

    protected DynamoDBClient(AmazonDynamoDB dynamoDB, String tableName, String primaryKey, String indexName, String sortKeyName, String dataField) {
        this.client = dynamoDB;
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.indexName = indexName;
        this.sortKeyName = sortKeyName;
        this.dataField = dataField;
    }

    protected QueryResult makeRequestForAttribute(String attributeName, String attributeValue) throws DatabaseClientException {
        // Set up mapping of the partition name with the value
        val attributeValues = Collections.singletonMap(attributeName, new AttributeValue().withS(attributeValue));
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

    protected ScanResult scanTable() {
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
        return client.scan(scanRequest);
    }

    protected  <T> List<T> parseResponseItems(List<java.util.Map<String, AttributeValue>> items, Class<T> valueType) throws DatabaseClientException {
        List<T> results = new ArrayList<>();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                try {
                    val data = item.get(dataField).getS();
                    results.add(mapper.readValue(data, valueType));
                } catch (Exception e) {
                    throw new DatabaseClientException("JSON Processing failed ", e);
                }
            }
        } else {
            throw new DatabaseClientException("Unable to parse the Query Result");
        }
        return results;
    }

}
