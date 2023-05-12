package uk.gov.justice.digital.test;

import lombok.val;
import uk.gov.justice.digital.domain.model.TableIdentifier;

import java.io.File;

/**
 * In order to speed up tests we provide some preloaded tables that are used as sources in the test. This avoids the
 * overhead of loading the tables for each test case.
 *
 * Run the main method in this class to regenerate the test data and then commit the contents of resources/TBD after
 * confirming that the tests are passing.
 */
public class GenerateTableData {

    // Resource directory where table data will be written. Tests will expect this data to be located here.
    public static final String TABLE_DATA_PATH = "src/test/resources/tables";
    public static final String DB_NAME = "test-db";

    public static void main(String[] args) {
        BaseSparkTest.createSession();
        val helpers = new SparkTestHelpers(BaseSparkTest.spark);
        val path = new File(TABLE_DATA_PATH).toPath();
        val absolutePath = path.toAbsolutePath().toString();

        helpers.persistDataset(
            new TableIdentifier(absolutePath, DB_NAME, "nomis", "offender_bookings"),
            helpers.getOffenderBookings(path)
        );

        helpers.persistDataset(
            new TableIdentifier(absolutePath, DB_NAME, "nomis", "offenders"),
            helpers.getOffenders(path)
        );

        helpers.persistDataset(
            new TableIdentifier(absolutePath, DB_NAME, "nomis", "agency_locations"),
            helpers.getAgencyLocations(path)
        );

        helpers.persistDataset(
            new TableIdentifier(absolutePath, DB_NAME, "nomis", "agency_internal_locations"),
            helpers.getInternalAgencyLocations(path)
        );

    }

}
