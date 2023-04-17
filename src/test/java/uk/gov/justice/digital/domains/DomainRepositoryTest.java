package uk.gov.justice.digital.domains;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.repository.DomainRepository;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class DomainRepositoryTest {

    protected static SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder()
                .config(sparkConf)
                .master("local[*]")
                .getOrCreate();
    }

    protected static InputStream getStream(final String resource) {
        InputStream stream = System.class.getResourceAsStream(resource);
        if(stream == null) {
            stream = System.class.getResourceAsStream("/src/test/resources" + resource);
            if(stream == null) {
                stream = System.class.getResourceAsStream("/target/test-classes" + resource);
                if(stream == null) {
                    Path root = Paths.get(".").normalize().toAbsolutePath();
                    stream = System.class.getResourceAsStream(root.toString() + "/src/test/resources" + resource);
                    if(stream == null) {
                        stream = BaseSparkTest.class.getResourceAsStream(resource);
                    }
                }
            }
        }
        return stream;
    }

    protected Path createFileFromResource(final String resource, final String filename) throws IOException {
        final InputStream stream = getStream(resource);
        final File f = Paths.get(filename).toFile();
        FileUtils.copyInputStreamToFile(stream, f);
        return Paths.get(f.getAbsolutePath());
    }

    public static File[] findFilesForId(File dir) {
        return dir.listFiles(pathname -> pathname.getName().contains(".parquet"));
    }

//    @Test
//    public void test_load() {
//        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
//        URL url1 = getClass().getResource("/domains/incident_domain.json");
//        URL url2 = getClass().getResource("/domain-repos");
//        assert url1 != null;
//        assert url2 != null;
//        final DomainRepository repository = new DomainRepository(spark, null);
//        File file = new File(url2.getPath() + "/domain-repository/domain");
//        assertTrue(findFilesForId(file).length > 0);
//    }


}
