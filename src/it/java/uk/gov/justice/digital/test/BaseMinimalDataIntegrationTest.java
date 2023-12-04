package uk.gov.justice.digital.test;

import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseMinimalDataIntegrationTest extends BaseSparkTest {
    protected static final int pk1 = 1;
    protected static final int pk2 = 2;
    protected static final int pk3 = 3;
    protected static final int pk4 = 4;
    protected static final int pk5 = 5;

    protected static final String inputSchemaName = "my-schema";
    protected static final String inputTableName = "my-table";

    @TempDir
    protected Path testRoot;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;
    protected String checkpointPath;

    protected void thenStructuredAndCuratedContainForPK(String data, int primaryKey) {
        assertStructuredAndCuratedForTableContainForPK(structuredPath, curatedPath, inputSchemaName, inputTableName, data, primaryKey);
    }

    protected void thenViolationsContainsForPK(String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath).resolve(inputSchemaName).resolve(inputTableName).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(violationsTablePath, data, primaryKey);
    }

    protected void thenStructuredAndCuratedDoNotContainPK(int primaryKey) {
        assertStructuredAndCuratedForTableDoNotContainPK(structuredPath, curatedPath, inputSchemaName, inputTableName, primaryKey);
    }


}
