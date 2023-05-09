package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
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
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.model.DomainDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import static uk.gov.justice.digital.job.model.Columns.DATA;

@Singleton
public class DynamoDBClient {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBClient.class);

    private final static String indexName = "secondaryId-type-index";
    private final static String sortKeyName = "secondaryId";

    private final AmazonDynamoDB dynamoDB;
    ObjectMapper mapper;

    @Inject
    public DynamoDBClient(JobParameters jobParameters) {
        this(AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        jobParameters.getAwsDynamoDBEndpointUrl(),
                        jobParameters.getAwsRegion()
                ))
                .build(), new ObjectMapper());
    }

    public DynamoDBClient(AmazonDynamoDB dynamoDB, ObjectMapper mapper) {
        this.dynamoDB = dynamoDB;
        this.mapper = mapper;
    }

    public DomainDefinition getDomainDefinition(final String domainTableName, final String domainId)
            throws PatternSyntaxException {

        String[] names = domainId.split("[.]");
        String domainName = names.length == 2 ? names[0] : domainId;
        String tableName = names.length == 2 ? names[1] : null;
        QueryResult response = executeQuery(domainTableName, domainName);
        return parse(response, tableName);
    }

    public DomainDefinition parse(QueryResult response, String tableName) {
        DomainDefinition domainDef = null;
        try {
            if (response != null) {
                for (Map<String, AttributeValue> items : response.getItems()) {
                    String data = items.get(DATA).getS();
                    domainDef = mapper.readValue(data, DomainDefinition.class);
                    if (tableName != null) {
                        domainDef.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(tableName));
                    }
                }
            }
        } catch (JsonProcessingException e) {
            // TODO handle exception properly
            logger.error("DynamoDB request failed:", e);
            return domainDef;
        }
        return domainDef;
    }

    public QueryResult executeQuery(final String domainTableName, final String domainName) {
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
            // TODO handle exception properly
            logger.error("DynamoDB request failed:" + e.getMessage());
            return null;
        }
    }

}
