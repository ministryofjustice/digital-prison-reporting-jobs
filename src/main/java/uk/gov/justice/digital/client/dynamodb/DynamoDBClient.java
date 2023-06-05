package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class DynamoDBClient {

    private final AmazonDynamoDB client;
    private final String tableName;

    private final String primaryKey;

    private final String indexName;

    protected final String sortKeyName;

    private final String dataField;
    protected static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBClient.class);

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

    protected Map<String, AttributeValue> getItem(final String key) {
        GetItemResult response = client.getItem(this.tableName, Collections.singletonMap(primaryKey, new AttributeValue().withS(key)));
        return (response != null) ? response.getItem() : Collections.<String, AttributeValue>emptyMap();
    }

    protected AttributeValue getItemValue(final String key, final String attribute) {
        GetItemResult response = client.getItem(this.tableName, Collections.singletonMap(primaryKey, new AttributeValue().withS(key)));
        return (response != null) ? response.getItem().get(attribute) : null;
    }

    protected boolean hasItem(final String key) {
        Map<String,AttributeValue> result = getItem(key);
        return result == null ? false : !result.isEmpty();
    }

    protected void putItem(Map<String, AttributeValue> values) {
        client.putItem(this.tableName, values);
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
            logger.error("DynamoDB client request failed");
            throw new DatabaseClientException("DynamoDB client request failed", e);
        }

    }

    protected <T> List<T> parseResponse(QueryResult response, Class<T> valueType) throws DatabaseClientException {
        List<T> results = new ArrayList<>();
        if (response != null) {
            for (Map<String, AttributeValue> items : response.getItems()) {
                try {
                    val data = items.get(dataField).getS();
                    results.add(mapper.readValue(data, valueType));
                } catch (JsonProcessingException e) {
                    logger.error("JSON Processing failed");
                    throw new DatabaseClientException("JSON Processing failed ", e);
                }
            }
        } else {
            logger.error("Unable to parse the Query Result");
            throw new DatabaseClientException("Unable to parse the Query Result");
        }
        return results;
    }

}
