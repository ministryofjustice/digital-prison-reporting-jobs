package uk.gov.justice.digital.client.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import uk.gov.justice.digital.client.ClientProvider;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class DynamoDbVelocityReportingClient {

    public static final String TABLE_KEY = "table";
    public static final String COUNT_KEY = "count";
    public static final String EVENTS_KEY = "events";
    public static final String BATCH_TIME_TOTAL_KEY = "batchTimeTot";
    public static final String BATCH_TIME_AVERAGE_KEY = "batchTimeAvg";
    public static final String CDC_ACTIVITY_KEY = "cdcActivity";
    public static final String LAST_UPDATED_KEY = "lastUpdated";

    // {
    //  "table": "/raw/nomis/offenders",
    //  "count" : 1237462, No of records by table
    //  "events": 2938474, No of events experienced by table (raw no records should = no events)
    //  "batchTimeAvg" : 324.55,  #milliseconds per record
    //                              Time taken to process a batch
    //  "batchSizeAvg" : 7,       #average CDC batch size, Average time per record
    //  "cdcActivity" : 8372,      #no of CDC events per day
    //  "lastUpdatedTime" : "2023-11-04T13:44:02Z"
    //}

    private final AmazonDynamoDB dynamoDbClient;
    private final String dynamoTableName;

    @Inject
    public DynamoDbVelocityReportingClient(ClientProvider<AmazonDynamoDB> clientProvider, JobArguments jobArguments) {
        this.dynamoDbClient = clientProvider.getClient();
        this.dynamoTableName = jobArguments.getDataVelocityTableName();
    }

    public void writeRecord(String tablePath, int count, int events, long batchTimeTotal, long batchTimeAvg, int cdcActivity, String timestamp) {
        AttributeValue tableAttribute = new AttributeValue(tablePath);
        AttributeValue countAttribute = new AttributeValue().withN(Long.toString(count));
        AttributeValue eventsAttribute = new AttributeValue().withN(Long.toString(events));
        AttributeValue batchTimeTotalAttribute = new AttributeValue().withN(Long.toString(batchTimeTotal));
        AttributeValue batchTimeAvgAttribute = new AttributeValue().withN(Long.toString(batchTimeAvg));
        AttributeValue cdcActivityAttribute = new AttributeValue().withN(Long.toString(cdcActivity));
        AttributeValue lastUpdatedAttribute = new AttributeValue(timestamp);

        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(TABLE_KEY, tableAttribute);
        itemValues.put(COUNT_KEY, countAttribute);
        itemValues.put(EVENTS_KEY, eventsAttribute);
        itemValues.put(BATCH_TIME_TOTAL_KEY, batchTimeTotalAttribute);
        itemValues.put(BATCH_TIME_AVERAGE_KEY, batchTimeAvgAttribute);
        itemValues.put(CDC_ACTIVITY_KEY, cdcActivityAttribute);
        itemValues.put(LAST_UPDATED_KEY, lastUpdatedAttribute);

        PutItemRequest putTokenRequest = new PutItemRequest(dynamoTableName, itemValues);
        dynamoDbClient.putItem(putTokenRequest);
    }
}
