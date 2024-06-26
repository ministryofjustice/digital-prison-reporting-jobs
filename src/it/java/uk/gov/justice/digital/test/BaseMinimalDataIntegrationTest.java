package uk.gov.justice.digital.test;

import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

public class BaseMinimalDataIntegrationTest extends BaseSparkTest {
    protected static final int pk1 = 1;
    protected static final int pk2 = 2;
    protected static final int pk3 = 3;
    protected static final int pk4 = 4;
    protected static final int pk5 = 5;
    protected static final int pk6 = 6;

    protected static final String inputSchemaName = "my_schema";
    protected static final String inputTableName = "my_table";

    @TempDir
    protected Path testRoot;
    protected String rawPath;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;
    protected String checkpointPath;

    protected void thenStructuredAndCuratedContainForPK(String data, int primaryKey) {
        assertStructuredAndCuratedForTableContainForPK(structuredPath, curatedPath, inputSchemaName, inputTableName, data, primaryKey);
    }

    protected void thenStructuredViolationsContainsPK(int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath)
                .resolve("structured")
                .resolve(inputSchemaName)
                .resolve(inputTableName)
                .toAbsolutePath()
                .toString();
        assertViolationsTableContainsPK(violationsTablePath, primaryKey);
    }

    protected void thenStructuredViolationsContainsForPK(String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath)
                .resolve("structured")
                .resolve(inputSchemaName)
                .resolve(inputTableName)
                .toAbsolutePath()
                .toString();
        assertViolationsTableContainsForPK(violationsTablePath, data, primaryKey);
    }

    protected void thenStructuredAndCuratedDoNotContainPK(int primaryKey) {
        assertStructuredAndCuratedForTableDoNotContainPK(structuredPath, curatedPath, inputSchemaName, inputTableName, primaryKey);
    }


}
