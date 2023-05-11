package uk.gov.justice.digital.client.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;
import uk.gov.justice.digital.config.JobParameters;


@Singleton
public class DynamoDBClientProvider implements ClientProvider<AmazonDynamoDB> {

    private final JobParameters jobParameters;
    @Inject
    public DynamoDBClientProvider(JobParameters jobParameters) {
        this.jobParameters = jobParameters;
    }

    @Override
    public AmazonDynamoDB getClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        jobParameters.getAwsDynamoDBEndpointUrl(),
                        jobParameters.getAwsRegion()
                ))
                .build();
    }
}
