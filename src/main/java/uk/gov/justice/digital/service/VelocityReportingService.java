package uk.gov.justice.digital.service;

import uk.gov.justice.digital.client.dynamo.DynamoDbVelocityReportingClient;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class VelocityReportingService {

    private final DynamoDbVelocityReportingClient client;

    @Inject
    public VelocityReportingService(DynamoDbVelocityReportingClient client) {
        this.client = client;
    }
}
