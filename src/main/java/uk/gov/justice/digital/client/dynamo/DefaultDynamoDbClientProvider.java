package uk.gov.justice.digital.client.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;
import uk.gov.justice.digital.config.JobArguments;

@Singleton
public class DefaultDynamoDbClientProvider implements ClientProvider<AmazonDynamoDB> {

    private final JobArguments arguments;

    public DefaultDynamoDbClientProvider(JobArguments arguments) {
        this.arguments = arguments;
    }

    @Override
    public AmazonDynamoDB getClient() {
        return AmazonDynamoDBClientBuilder.standard().withRegion(arguments.getAwsRegion()).build();
    }
}
