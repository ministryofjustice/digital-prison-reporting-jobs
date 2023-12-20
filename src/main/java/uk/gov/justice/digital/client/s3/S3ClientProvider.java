package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import uk.gov.justice.digital.client.ClientProvider;

import javax.inject.Singleton;

@Singleton
public class S3ClientProvider implements ClientProvider<AmazonS3> {
    @Override
    public AmazonS3 getClient() {
        return AmazonS3ClientBuilder.defaultClient();
    }
}
