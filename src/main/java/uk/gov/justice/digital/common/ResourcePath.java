package uk.gov.justice.digital.common;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;

import java.net.URI;
import java.net.URISyntaxException;

public class ResourcePath {

    // We do not use the `file.separator` property since these paths refer to remote resources e.g. s3://foo/bar as
    // well as local resources. If we run into issues with users running on windows this can be revisited but for now
    // this is sufficient.
    private static final String PATH_SEPARATOR = "/";
    private static final Logger logger = LoggerFactory.getLogger(ResourcePath.class);

    public static String createValidatedPath(String... elements) {
        val rawPath = String.join(PATH_SEPARATOR, elements);
        try {
            return new URI(rawPath)
                    .normalize()
                    .toASCIIString();
        } catch (URISyntaxException e) {
            logger.error("Failed to validate path : " + rawPath);
            throw new IllegalArgumentException("Failed to validate path: " + rawPath, e);
        }
    }

    private ResourcePath() { }
}
