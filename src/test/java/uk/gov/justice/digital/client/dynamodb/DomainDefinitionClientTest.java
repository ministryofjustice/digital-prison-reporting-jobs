package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class DomainDefinitionClientTest {

    private static DomainDefinitionClient underTest;
    private static AmazonDynamoDB dynamoDB;
    private static ObjectMapper mapper;


    @BeforeEach
    public void setUp() {
        dynamoDB = mock(AmazonDynamoDB.class);
        mapper = mock(ObjectMapper.class);
        underTest = new DomainDefinitionClient(dynamoDB, mapper);
    }

    @Test
    public void shouldExecuteQueryForGivenDomain() throws DatabaseClientException {
        val domainTableName = "test";
        val domainName = "incident";
        val result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        val queryResult = underTest.executeQuery(domainTableName, domainName);
        assertTrue(queryResult.getItems().isEmpty());
    }

    @Test
    public void shouldReturnErrorIfDomainTableDoesntExists() {
        val domainTableName = "test";
        val domainName = "incident";
        when(dynamoDB.query(any(QueryRequest.class))).thenThrow(AmazonDynamoDBException.class);
        assertThrows(
                DatabaseClientException.class,
                () -> underTest.executeQuery(domainTableName, domainName),
                "Expected executeQuery() to throw, but it didn't"
        );
    }

    @Test
    public void shouldParseQueryResultForGivenDomainName() throws IOException, DatabaseClientException {
        val result = mock(QueryResult.class);
        val domainDef = new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        val l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"test name\", " +
                                        "\"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        val data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        val expectedDomainDef = underTest.parse(result, null);
        assertEquals("test name", expectedDomainDef.getName());
    }

    @Test
    public void shouldParseQueryResultForGivenDomainAndTableName() throws IOException, DatabaseClientException {
        val result = mock(QueryResult.class);
        val domainDef = new DomainDefinition();
        domainDef.setName("living_unit");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        val l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"demographics\"," +
                                        " \"description\": \"test description\"}")
                                .withS("{\"id\": \"123\", \"name\": \"living_unit\", " +
                                        "\"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        val data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenReturn(domainDef);
        val expectedDomainDef = underTest.parse(result, "demographics");
        assertEquals("living_unit", expectedDomainDef.getName());
    }

    @Test
    public void shouldParseQueryResultProducesException() throws IOException {
        val result = mock(QueryResult.class);
        val domainDef = new DomainDefinition();
        domainDef.setName("test name");
        domainDef.setId("123");
        domainDef.setDescription("test description");
        val l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"test name\"," +
                                        " \"description\": \"test description\"}")));
        when(result.getItems()).thenReturn(l);
        val data = l.get(0).get("data").getS();
        when(mapper.readValue(data, DomainDefinition.class)).thenThrow(JsonProcessingException.class);
        assertThrows(
                DatabaseClientException.class,
                () -> underTest.parse(result, null),
                "Expected parse() to throw, but it didn't"
        );
    }
}