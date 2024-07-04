package uk.gov.justice.digital.test;

import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

import static uk.gov.justice.digital.test.SharedTestFunctions.assertOperationalDataStoreContainsForPK;
import static uk.gov.justice.digital.test.SharedTestFunctions.assertOperationalDataStoreDoesNotContainPK;

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

    protected void thenStructuredCuratedAndOperationalDataStoreContainForPK(String data, int primaryKey, Connection testQueryConnection) throws SQLException {
        assertStructuredAndCuratedForTableContainForPK(structuredPath, curatedPath, inputSchemaName, inputTableName, data, primaryKey);
        assertOperationalDataStoreContainsForPK(inputSchemaName, inputTableName, data, primaryKey, testQueryConnection);
    }

    protected void thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(int primaryKey, Connection testQueryConnection) throws SQLException {
        assertStructuredAndCuratedForTableDoNotContainPK(structuredPath, curatedPath, inputSchemaName, inputTableName, primaryKey);
        assertOperationalDataStoreDoesNotContainPK(inputSchemaName, inputTableName, primaryKey, testQueryConnection);
    }
}
