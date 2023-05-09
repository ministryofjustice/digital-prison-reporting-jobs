package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.ResolvedType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DynamoDBClientTest {


    private static final long serialVersionUID = 1;
    private static DynamoDBClient dynamoDBClient;
    private static final AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
    private static final ObjectMapper mapper = mock(ObjectMapper.class);


    @BeforeEach
    void setUp() {
        dynamoDBClient = new DynamoDBClient(dynamoDB, mapper);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void test_executeQuery() {
        String domainTableName = "test";
        String domainName = "incident";
        QueryResult result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        QueryResult expected_result = dynamoDBClient.executeQuery(domainTableName, domainName);
        assert(expected_result.getItems().isEmpty());
    }


    @Test
    @SuppressWarnings("serial")
    void test_getDomainDefinition() throws IOException {
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        QueryResult result = mock(QueryResult.class);
        when(dynamoDBClient.executeQuery("test", "incident")).thenReturn(result);
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {{
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"test name\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        when(dynamoDBClient.parse(result, null)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = dynamoDBClient.getDomainDefinition("test",
                "incident");
        verify(dynamoDB, times(1)).query(any());
        verify(mapper, times(1)).readValue(data, DomainDefinition.class);
        assertEquals(expectedDomainDef.getName(), "test name");
    }


    @Test
    @SuppressWarnings("serial")
    void test_parse() throws IOException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {{
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"test name\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = dynamoDBClient.parse(result, null);
        assertEquals(expectedDomainDef.getName(), "test name");
    }
}