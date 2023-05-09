package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DynamoDBClientTest {


    private static final Logger logger = LoggerFactory.getLogger(DynamoDBClientTest.class);

    private static final long serialVersionUID = 1;
    private static DynamoDBClient dynamoDBClient;
    private static AmazonDynamoDB dynamoDB;
    private static ObjectMapper mapper;


    @BeforeEach
    void setUp() {
        dynamoDB = mock(AmazonDynamoDB.class);
        mapper = mock(ObjectMapper.class);
        dynamoDBClient = new DynamoDBClient(dynamoDB, mapper);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void shouldExecuteQueryForGivenDomain() {
        String domainTableName = "test";
        String domainName = "incident";
        QueryResult result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        QueryResult expected_result = dynamoDBClient.executeQuery(domainTableName, domainName);
        assert(expected_result.getItems().isEmpty());
    }

    @Test
    void shouldReturnErrorIfDomainTableDoesntExists() {
        String domainTableName = "test";
        String domainName = "incident";
        QueryResult result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenThrow(ResourceNotFoundException.class);
        try {
            QueryResult expected_result = dynamoDBClient.executeQuery(domainTableName, domainName);
            fail();
        } catch (AmazonDynamoDBException e) {
            logger.info("Database not exists test");
            logger.error(e.getMessage());
            assertTrue(true);
        }
    }


    @Test
    @SuppressWarnings("serial")
    void shouldReturnDomainDefinitionForGivenDomain() throws IOException {
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
        verify(mapper, times(1)).readValue(data, DomainDefinition.class);
        assertEquals(expectedDomainDef.getName(), "test name");
    }


    @Test
    @SuppressWarnings("serial")
    void shouldParseQueryResultForGivenDomainName() throws IOException {
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