package uk.gov.justice.digital.common;

import lombok.val;
import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.String.format;

public class ResourcePath {

    // We do not use the `file.separator` property since these paths refer to remote resources e.g. s3://foo/bar as
    // well as local resources. If we run into issues with users running on windows this can be revisited but for now
    // this is sufficient.
    private static final String PATH_SEPARATOR = "/";

    public static String createValidatedPath(String... elements) {
        val rawPath = String.join(PATH_SEPARATOR, elements);
        try {
            return new URI(rawPath)
                    .normalize()
                    .toASCIIString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to validate path: " + rawPath, e);
        }
    }

    public static String tablePath(String rootPath, String source, String table) {
        return format("%s%s/%s/", ensureEndsWithSlash(rootPath), source, table);
    }

    public static String ensureEndsWithSlash(String originalString) {
        if (originalString.endsWith("/")) {
            return originalString;
        } else {
            return originalString + "/";
        }
    }

    private ResourcePath() { }
}
