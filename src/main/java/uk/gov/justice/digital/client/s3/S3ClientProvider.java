package uk.gov.justice.digital.client.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import uk.gov.justice.digital.client.ClientProvider;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class S3ClientProvider implements ClientProvider<AmazonS3> {

    private final JobArguments jobArguments;

    @Inject
    public S3ClientProvider(JobArguments jobArguments) {
        this.jobArguments = jobArguments;
    }

    @Override
    public AmazonS3 getClient() {
        // The maximum number of connections to S3
        // Give us some wiggle room just in case there are other tasks
        int extraBufferForOtherTasks = 10;
        int maxConnections = jobArguments.getFileTransferParallelism() + extraBufferForOtherTasks;

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(maxConnections);
        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(clientConfiguration)
                .build();
    }
}
