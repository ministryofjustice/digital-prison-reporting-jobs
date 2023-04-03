package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.justice.digital.domains.model.DomainDefinition;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.job.model.Columns.DATA;

@Singleton
public class DynamoDBClient {

    private final static ObjectMapper MAPPER = new ObjectMapper();
    // TODO hardcoded values
    private final String indexName = "secondaryId-type-index";
    private final String sortKeyName = "secondaryId";
    private final AmazonDynamoDB dynamoDB;

    public DynamoDBClient() {
        dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }

    public DomainDefinition getDomainDefinition(final String domainTableName, final String domainId) {
        String[] names = domainId.split("[.]");
        String domainName = names.length >= 2?names[0]:domainId;

        //set up mapping of the partition name with the value
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":"+ sortKeyName, new AttributeValue().withS(domainName));

        QueryRequest queryReq = new QueryRequest()
                .withTableName(domainTableName)
                .withIndexName(indexName)
                .withKeyConditionExpression(sortKeyName + " = :" + sortKeyName)
                .withExpressionAttributeValues(attrValues);
        DomainDefinition domainDef = null;
        try {
            QueryResult response = dynamoDB.query(queryReq);

            for(Map<String, AttributeValue> items : response.getItems()) {
                String data = items.get(DATA).getS();
                domainDef = MAPPER.readValue(data, DomainDefinition.class);
                if(names.length >= 2) {
                    domainDef.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(names[1]));
                }
            }
            return domainDef;
        } catch (AmazonDynamoDBException | JsonProcessingException e) {
            // TODO handle exception properly
            System.err.println(e.getMessage());
            return domainDef;
        }
    }
}
