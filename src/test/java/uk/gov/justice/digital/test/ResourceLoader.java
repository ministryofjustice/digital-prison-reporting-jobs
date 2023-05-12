package uk.gov.justice.digital.test;

import java.io.InputStream;
import java.util.Optional;
import java.util.Scanner;

public class ResourceLoader {

	public static String getResource(String resource) {
		return Optional.ofNullable(System.class.getResourceAsStream(resource))
			.map(ResourceLoader::readInputStream)
			.orElseThrow(() -> new IllegalStateException("Failed to read resource: " + resource));
	}

	private static String readInputStream(InputStream is) {
		return new Scanner(is, "UTF-8")
			.useDelimiter("\\A") // Matches the next boundary which in our case will be EOF.
			.next();
	}

}
