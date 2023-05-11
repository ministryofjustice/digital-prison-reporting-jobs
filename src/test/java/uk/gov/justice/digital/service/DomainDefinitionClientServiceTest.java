package uk.gov.justice.digital.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.service.DomainDefinitionClientService;
import uk.gov.justice.digital.service.DomainService;

import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DomainDefinitionClientServiceTest {


    private static DomainDefinitionClientService domainDefinitionService;
    private static DomainService domainService;
    private static AmazonDynamoDB dynamoDB;
    private static ObjectMapper mapper;


    @BeforeEach
    void setUp() {
        dynamoDB = mock(AmazonDynamoDB.class);
        mapper = mock(ObjectMapper.class);
        domainDefinitionService = new DomainDefinitionClientService(dynamoDB, mapper);
        domainService = new DomainService(mock(JobParameters.class), domainDefinitionService,
                mock(DomainExecutor.class));
    }

    @Test
    void shouldExecuteQueryForGivenDomain() {
        String domainTableName = "test";
        String domainName = "incident";
        QueryResult result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        QueryResult expected_result = domainDefinitionService.executeQuery(domainTableName, domainName);
        assert(expected_result.getItems().isEmpty());
    }

    @Test
    void shouldReturnErrorIfDomainTableDoesntExists() {
        String domainTableName = "test";
        String domainName = "incident";
        when(dynamoDB.query(any(QueryRequest.class))).thenThrow(ResourceNotFoundException.class);
        try {
            QueryResult expected_result = domainDefinitionService.executeQuery(domainTableName, domainName);
            fail();
        } catch (AmazonDynamoDBException e) {
            assertTrue(true);
        }
    }


    @Test
    void shouldReturnDomainDefinitionForGivenDomain() throws IOException {
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        QueryResult result = mock(QueryResult.class);
        when(domainDefinitionService.executeQuery("test", "incident")).thenReturn(result);
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {
            static final long serialVersionUID = -2338626292552177485L;
            {
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"test name\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        when(domainDefinitionService.parse(result, null)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = domainService.getDomainDefinition("test",
                "incident", "demographics");
        verify(mapper, times(1)).readValue(data, DomainDefinition.class);
        assertEquals(expectedDomainDef.getName(), "test name");
    }


    @Test
    void shouldParseQueryResultForGivenDomainName() throws IOException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {
            static final long serialVersionUID = -2338626292552177485L;
            {
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"test name\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = domainDefinitionService.parse(result, null);
        assertEquals(expectedDomainDef.getName(), "test name");
    }

    @Test
    void shouldParseQueryResultForGivenDomainAndTableName() throws IOException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("living_unit");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {
            static final long serialVersionUID = -2338626292552177485L;
            {
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"demographics\", \"description\": \"test description\"}")
                    .withS("{\"id\": \"123\", \"name\": \"living_unit\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        DomainDefinition expectedDomainDef = domainDefinitionService.parse(result, "demographics");
        assertEquals(expectedDomainDef.getName(), "living_unit");
    }

    @Test
    void shouldParseQueryResultProducesException() throws IOException {
        QueryResult result = mock(QueryResult.class);
        DomainDefinition domainDef =  new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        List<Map<String, AttributeValue>> l = Collections.singletonList(new HashMap<String, AttributeValue>() {
            static final long serialVersionUID = -2338626292552177485L;
            {
            put("data", new AttributeValue()
                    .withS("{\"id\": \"123\", \"name\": \"test name\", \"description\": \"test description\"}"));
        }});
        when(result.getItems()).thenReturn(l);
        String data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenThrow(JsonProcessingException.class);
        try {
            DomainDefinition expectedDomainDef = domainDefinitionService.parse(result, null);
            fail();
        } catch (JsonProcessingException e) {
            assertTrue(true);
        }
    }
}