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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import static uk.gov.justice.digital.job.model.Columns.DATA;

@Singleton
public class DomainDefinitionDB {

    private static final Logger logger = LoggerFactory.getLogger(DomainDefinitionDB.class);

    private final static String indexName = "secondaryId-type-index";
    private final static String sortKeyName = "secondaryId";

    private final AmazonDynamoDB dynamoDB;
    ObjectMapper mapper;

    @Inject
    public DomainDefinitionDB(DynamoDBClientProvider dynamoDBClientProvider) {
        this(dynamoDBClientProvider.getClient(), new ObjectMapper());
    }

    public DomainDefinitionDB(AmazonDynamoDB dynamoDB, ObjectMapper mapper) {
        this.dynamoDB = dynamoDB;
        this.mapper = mapper;
    }

    public DomainDefinition getDomainDefinition(final String domainRegistry,
                                                final String domainName, final String tableName)
            throws PatternSyntaxException {
        try {
            QueryResult response = executeQuery(domainRegistry, domainName);
            return parse(response, tableName);
        } catch (AmazonDynamoDBException | JsonProcessingException e){
            // TODO handle exception properly
            logger.error("DynamoDB request failed:" + e.getMessage());
            return null;
        }
    }

    public DomainDefinition parse(QueryResult response, String tableName) throws JsonProcessingException {
        DomainDefinition domainDef = null;
        if (response != null) {
            for (Map<String, AttributeValue> items : response.getItems()) {
                String data = items.get(DATA).getS();
                domainDef = mapper.readValue(data, DomainDefinition.class);
                if (tableName != null) {
                    domainDef.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(tableName));
                }
            }
        }
        return domainDef;
    }

    public QueryResult executeQuery(final String domainTableName, final String domainName)
            throws AmazonDynamoDBException{
        // Set up mapping of the partition name with the value
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":" + sortKeyName, new AttributeValue().withS(domainName));
        QueryRequest queryReq = new QueryRequest()
                .withTableName(domainTableName)
                .withIndexName(indexName)
                .withKeyConditionExpression(sortKeyName + " = :" + sortKeyName)
                .withExpressionAttributeValues(attrValues);
        return dynamoDB.query(queryReq);
    }
}
