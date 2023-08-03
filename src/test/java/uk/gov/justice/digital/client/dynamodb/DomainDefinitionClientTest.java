package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class DomainDefinitionClientTest {

    private static DomainDefinitionClient underTest;
    private static AmazonDynamoDB dynamoDB;
    private static DynamoDBClientProvider provider;
    private static JobArguments args;

    private static final String domainTableName = "test";
    private static final String domainName = "incident";

    @BeforeEach
    public void setUp() {
        provider = mock(DynamoDBClientProvider.class);
        dynamoDB = mock(AmazonDynamoDB.class);
        args = mock(JobArguments.class);

        when(args.getDomainRegistry()).thenReturn(domainTableName);
        when(provider.getClient()).thenReturn(dynamoDB);

        underTest = new DomainDefinitionClient(provider, args);
    }

    @Test
    public void shouldReturnErrorIfDomainTableDoesntExist() {
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
        domainDef.setName(domainName);
        domainDef.setId("123");
        domainDef.setDescription("test description");
        val l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{\"id\": \"123\", \"name\": \"" + domainName + "\", " +
                                        "\"description\": \"test description\"," +
                                        "\"tables\": [{\"name\": \"" + domainTableName + "\"}]}")));
        when(result.getItems()).thenReturn(l);
        when(dynamoDB.query(any())).thenReturn(result);
        val actualDomainDefinition = underTest.getDomainDefinition(domainName, domainTableName);
        assertEquals(domainName, actualDomainDefinition.getName());
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
                                .withS("{\"id\": \"123\", \"name\": \"living_unit\", " +
                                        "\"description\": \"test description\"," +
                                        "\"tables\": [{\"name\": \"" + domainTableName + "\"}]}")));
        when(result.getItems()).thenReturn(l);
        when(dynamoDB.query(any())).thenReturn(result);

        val actualDomainDefinition = underTest.getDomainDefinition("living_unit", domainTableName);
        assertEquals("living_unit", actualDomainDefinition.getName());
        assertIterableEquals(
                Collections.singletonList(domainTableName),
                actualDomainDefinition.getTables().stream().map(TableDefinition::getName).collect(Collectors.toList())
        );
    }

    @Test
    public void shouldThrowAnExceptionGivenAnInvalidResponse() {
        val result = mock(QueryResult.class);
        val domainDef = new DomainDefinition();
        domainDef.setName(domainName);
        domainDef.setId("123");
        domainDef.setDescription("test description");
        val l = Collections
                .singletonList(
                        Collections.singletonMap("data", new AttributeValue()
                                .withS("{ invalid response }")));
        when(result.getItems()).thenReturn(l);
        when(dynamoDB.query(any())).thenReturn(result);
        assertThrows(
                DatabaseClientException.class,
                () -> underTest.getDomainDefinition(domainName, domainTableName),
                "Expected parse() to throw, but it didn't"
        );
    }

    @Test
    public void shouldParseRetrievedListOfDomainDefinitions() throws DatabaseClientException {
        val result = mock(ScanResult.class);
        val expectedDomainNames = new ArrayList<String>();
        expectedDomainNames.add("living_unit");
        expectedDomainNames.add("agency");

        val domainScanResult = new ArrayList<Map<String, AttributeValue>>();
        domainScanResult.add(
                Collections.singletonMap("data", new AttributeValue()
                        .withS("{\"id\": \"123\", \"name\": \"living_unit\", " +
                                "\"description\": \"test description\"," +
                                "\"tables\": [" +
                                "{\"name\": \"" + domainTableName + "\"}," +
                                "{\"name\": \"table_1\"}" +
                                "]}"))
        );
        domainScanResult.add(
                Collections.singletonMap("data", new AttributeValue()
                        .withS("{\"id\": \"123\", \"name\": \"agency\", " +
                                "\"description\": \"test description\"," +
                                "\"tables\": [" +
                                "{\"name\": \"table_2\"}," +
                                "{\"name\": \"" + domainTableName + "\"}" +
                                "]}"))
        );

        when(result.getItems()).thenReturn(domainScanResult);
        when(dynamoDB.scan(any())).thenReturn(result);

        val actualDomainDefinitions = underTest.getDomainDefinitions();

        assertIterableEquals(
                expectedDomainNames,
                actualDomainDefinitions.stream().map(DomainDefinition::getName).collect(Collectors.toList())
        );

        assertTrue(
                actualDomainDefinitions
                        .stream()
                        .allMatch(domain ->
                                domain.getTables()
                                        .stream()
                                        .anyMatch(table -> table.getName().endsWith(domainTableName))
                        )
        );
    }
}