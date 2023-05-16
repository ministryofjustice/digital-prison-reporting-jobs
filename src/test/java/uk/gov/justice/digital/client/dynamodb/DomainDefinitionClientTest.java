package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;
import uk.gov.justice.digital.exception.DomainServiceException;
import uk.gov.justice.digital.service.DomainService;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DomainDefinitionClientTest {


    private static DomainDefinitionClient domainDefinitionService;
    private static DomainService domainService;
    private static AmazonDynamoDB dynamoDB;
    private static ObjectMapper mapper;


    @BeforeEach
    void setUp() {
        dynamoDB = mock(AmazonDynamoDB.class);
        mapper = mock(ObjectMapper.class);
        domainDefinitionService = new DomainDefinitionClient(dynamoDB, mapper);
        domainService = new DomainService(mock(JobParameters.class), domainDefinitionService,
                mock(DomainExecutor.class));
    }

    @Test
    void shouldExecuteQueryForGivenDomain() throws DatabaseClientException {
        String domainTableName = "test";
        String domainName = "incident";
        QueryResult result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        QueryResult expected_result = domainDefinitionService.executeQuery(domainTableName, domainName);
        assertTrue(expected_result.getItems().isEmpty());
    }

    @Test
    void shouldReturnErrorIfDomainTableDoesntExists() {
        String domainTableName = "test";
        String domainName = "incident";
        when(dynamoDB.query(any(QueryRequest.class))).thenThrow(AmazonDynamoDBException.class);
        DatabaseClientException thrown = assertThrows(
                DatabaseClientException.class,
                () -> domainDefinitionService.executeQuery(domainTableName, domainName),
                "Expected executeQuery() to throw, but it didn't"
        );
    }


    @Test
    void shouldReturnDomainDefinitionForGivenDomain() throws DatabaseClientException, DomainServiceException,
            JsonProcessingException {
        DomainDefinition domainDef = new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        QueryResult result = mock(QueryResult.class);
        when(domainDefinitionService.executeQuery("test", "incident")).thenReturn(result);
        List<Map<String, AttributeValue>> l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"test name\", " +
                                        "\"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        when(domainDefinitionService.parse(result, null)).thenReturn(domainDef);
        Set<DomainDefinition> actualDomains = domainService.getDomains("test",
                "incident.demographics");
        verify(mapper, times(1)).readValue(data, DomainDefinition.class);
        for (DomainDefinition actualDomain : actualDomains)
            assertEquals("test name", actualDomain.getName());
    }


    @Test
    void shouldParseQueryResultForGivenDomainName() throws IOException, DatabaseClientException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef = new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"test name\", " +
                                        "\"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = domainDefinitionService.parse(result, null);
        assertEquals("test name", expectedDomainDef.getName());
    }

    @Test
    void shouldParseQueryResultForGivenDomainAndTableName() throws IOException, DatabaseClientException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef = new DomainDefinition();
        domainDef.setName("living_unit");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"demographics\"," +
                                        " \"description\": \"test description\"}")
                                .withS("{\"id\": \"123\", \"name\": \"living_unit\", " +
                                        "\"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = domainDefinitionService.parse(result, "demographics");
        assertEquals("living_unit", expectedDomainDef.getName());
    }

    @Test
    void shouldParseQueryResultProducesException() throws IOException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef = new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"test name\"," +
                                        " \"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenThrow(JsonProcessingException.class);
        DatabaseClientException thrown = assertThrows(
                DatabaseClientException.class,
                () -> domainDefinitionService.parse(result, null),
                "Expected parse() to throw, but it didn't"
        );
    }
}