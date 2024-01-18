package uk.gov.justice.digital.client.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.ClientProvider;
import uk.gov.justice.digital.config.JobArguments;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.BATCH_TIME_AVERAGE_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.BATCH_TIME_TOTAL_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.CDC_ACTIVITY_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.COUNT_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.EVENTS_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.LAST_UPDATED_KEY;
import static uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient.TABLE_KEY;
import static uk.gov.justice.digital.test.matchers.DynamoAttributeMatchers.hasNumberValue;
import static uk.gov.justice.digital.test.matchers.DynamoAttributeMatchers.hasStringValue;

@ExtendWith(MockitoExtension.class)
class DynamoDbVelocityReportingClientTest {

    private static final String TABLE_NAME = "a-table";

    @Mock
    private ClientProvider<AmazonDynamoDB> clientProvider;
    @Mock
    private AmazonDynamoDB underlyingClient;
    @Captor
    private ArgumentCaptor<PutItemRequest> putItemRequestCaptor;
    @Mock
    private JobArguments arguments;

    private DynamoDbVelocityReportingClient underTest;

    @BeforeEach
    public void setUp() {
        when(clientProvider.getClient()).thenReturn(underlyingClient);
        when(arguments.getDataVelocityTableName()).thenReturn(TABLE_NAME);
        underTest = new DynamoDbVelocityReportingClient(clientProvider, arguments);
    }

    @Test
    public void shouldWriteAttributesToDynamoDb() {
        String tablePath = "/raw/nomis/offenders";
        int count = 1237462;
        int events = 2938474;
        long batchTimeTotal = 23456789L;
        long batchTimeAvg = 324L;
        int cdcActivity = 8372;
        String timestamp = "2023-11-04T13:44:02Z";

        underTest.writeRecord(tablePath, count, events, batchTimeTotal, batchTimeAvg, cdcActivity, timestamp);

        verify(underlyingClient, times(1)).putItem(putItemRequestCaptor.capture());

        PutItemRequest actualRequest = putItemRequestCaptor.getValue();
        Map<String, AttributeValue> actualAttributes = actualRequest.getItem();

        int expectedNumberOfAttributes = 7;
        assertEquals(expectedNumberOfAttributes, actualAttributes.size());

        assertThat(actualAttributes, hasEntry(is(TABLE_KEY), hasStringValue(tablePath)));
        assertThat(actualAttributes, hasEntry(is(COUNT_KEY), hasNumberValue(count)));
        assertThat(actualAttributes, hasEntry(is(EVENTS_KEY), hasNumberValue(events)));
        assertThat(actualAttributes, hasEntry(is(BATCH_TIME_TOTAL_KEY), hasNumberValue(batchTimeTotal)));
        assertThat(actualAttributes, hasEntry(is(BATCH_TIME_AVERAGE_KEY), hasNumberValue(batchTimeAvg)));
        assertThat(actualAttributes, hasEntry(is(CDC_ACTIVITY_KEY), hasNumberValue(cdcActivity)));
        assertThat(actualAttributes, hasEntry(is(LAST_UPDATED_KEY), hasStringValue(timestamp)));

        assertEquals(TABLE_NAME, actualRequest.getTableName());
    }

}