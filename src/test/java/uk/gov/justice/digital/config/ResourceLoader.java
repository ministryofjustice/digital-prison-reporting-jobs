package uk.gov.justice.digital.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;

public class ResourceLoader {
	
	@SuppressWarnings("deprecation")
	public static String getResource(final Class<?> clazz, final String resource) throws IOException {
		final InputStream stream = ResourceLoader.getStream(clazz, resource);
		return IOUtils.toString(stream);
	}

	public static InputStream getStream(final Class<?> clazz, final String resource) {
		InputStream stream = System.class.getResourceAsStream(resource);
		if(stream == null) {
			stream = System.class.getResourceAsStream("/src/test/resources" + resource);
			if(stream == null) {
				stream = System.class.getResourceAsStream("/target/test-classes" + resource);
				if(stream == null) {
					Path root = Paths.get(".").normalize().toAbsolutePath();
					stream = System.class.getResourceAsStream(root.toString() + "/src/test/resources" + resource);
					if(stream == null) {
						stream = clazz.getResourceAsStream(resource);
					}
				}
			}
		}
		return stream;
	}
}
