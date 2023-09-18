package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class DynamoDBClient implements Serializable {

    private static final long serialVersionUID = 3057998036760385193L;

    private final DynamoDBClientProvider dynamoDBClientProvider;

    // AmazonDynamoDB object requires special handling for Spark checkpointing since it is not serializable.
    // Transient ensures Java does not attempt to serialize it.
    // Volatile ensures it keeps the 'final' concurrency initialization guarantee.
    private transient volatile AmazonDynamoDB client;
    private final String tableName;

    private final String primaryKey;

    private final String indexName;

    protected final String sortKeyName;

    private final String dataField;
    protected static final ObjectMapper mapper = new ObjectMapper();

    protected DynamoDBClient(DynamoDBClientProvider dynamoDBClientProvider, String tableName, String primaryKey) {
        this(dynamoDBClientProvider, tableName, primaryKey, null, null, null);
    }

    protected DynamoDBClient(DynamoDBClientProvider dynamoDBClientProvider, String tableName, String primaryKey, String indexName, String sortKeyName, String dataField) {
        this.dynamoDBClientProvider = dynamoDBClientProvider;
        this.client = dynamoDBClientProvider.getClient();
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

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.client = this.dynamoDBClientProvider.getClient();
    }
}
