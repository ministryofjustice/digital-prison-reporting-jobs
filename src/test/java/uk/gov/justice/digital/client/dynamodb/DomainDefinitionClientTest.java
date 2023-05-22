package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DomainDefinitionClientTest {

    private static DomainDefinitionClient underTest;
    private static AmazonDynamoDB dynamoDB;

    @BeforeEach
    public void setUp() {
        dynamoDB = mock(AmazonDynamoDB.class);
        underTest = new DomainDefinitionClient(dynamoDB, "Some table name");
    }

    @Test
    public void shouldExecuteQueryForGivenDomain() throws DatabaseClientException {
        val domainTableName = "test";
        val domainName = "incident";
        val result = mock(QueryResult.class);
        when(dynamoDB.query(any(QueryRequest.class))).thenReturn(result);
        underTest.getDomainDefinition(domainTableName, domainName);
    }

    @Test
    public void shouldReturnErrorIfDomainTableDoesntExist() {
        val domainTableName = "test";
        val domainName = "incident";
        when(dynamoDB.query(any(QueryRequest.class))).thenThrow(AmazonDynamoDBException.class);
        assertThrows(
                DatabaseClientException.class,
                () -> underTest.getDomainDefinition(domainTableName, domainName),
                "Expected executeQuery() to throw, but it didn't"
        );
    }

    @Test
    public void shouldParseQueryResultForGivenDomainName() throws DatabaseClientException {
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
        when(dynamoDB.query(any())).thenReturn(result);
        val expectedDomainDef = underTest.getDomainDefinition("test name", null);
        assertEquals("test name", expectedDomainDef.getName());
    }

    @Test
    public void shouldParseQueryResultForGivenDomainAndTableName() throws DatabaseClientException {
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
        when(dynamoDB.query(any())).thenReturn(result);

        val response = underTest.getDomainDefinition("living_unit", "demographics");
        assertEquals("living_unit", response.getName());
        // Confirm that getTables was called which allows us to verify that the table filtering was attempted.
        verify(result).getItems();
    }

    @Test
    public void shouldThrowAnExceptionGivenAnInvalidResponse() {
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
        assertThrows(
                DatabaseClientException.class,
                () -> underTest.getDomainDefinition("test name", null),
                "Expected parse() to throw, but it didn't"
        );
    }
}