package uk.gov.justice.digital.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.shaded.com.nimbusds.jose.util.IOUtils;
import org.apache.spark.sql.types.DataType;

import com.google.common.base.Charsets;

public class SourceReference {

    protected static InputStream getStream(final String resource) {
        InputStream stream = System.class.getResourceAsStream(resource);
        if(stream == null) {
            stream = System.class.getResourceAsStream("/src/main/resources" + resource);
            if(stream == null) {
                stream = System.class.getResourceAsStream("/target/classes" + resource);
                if(stream == null) {
                    Path root = Paths.get(".").normalize().toAbsolutePath();
                    stream = System.class.getResourceAsStream(root.toString() + "/src/main/resources" + resource);
                    if(stream == null) {
                        stream = SourceReference.class.getResourceAsStream(resource);
                    }
                }
            }
        }
        return stream;
    }

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

    protected static DataType getSchemaFromResource(final String resource) throws IOException {
        final InputStream stream = getStream(resource);
        if(stream != null) {
            return DataType.fromJson(IOUtils.readInputStreamToString(stream, Charsets.UTF_8));
        }
        return null;
    }

    public static void main(String[] args) throws IOException {

        InputStream in = getStream("/sources/sources_data.json");
        System.out.println(in);
        DataType d = getSchemaFromResource("/sources/sources_data.json");
        System.out.println(d);
    }

}
