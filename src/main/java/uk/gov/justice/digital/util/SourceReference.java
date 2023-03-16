package uk.gov.justice.digital.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SourceReference {
    public static String getInternalSource(String actualSource) {
        try (InputStream sourceData = new FileInputStream("src/main/resources/sources/sources_data.properties")) {

            Properties prop = new Properties();
            prop.load(sourceData);

           return prop.getProperty(actualSource.toLowerCase());
        } catch (IOException ex) {
            ex.printStackTrace();
            return actualSource;
        }
    }

}
