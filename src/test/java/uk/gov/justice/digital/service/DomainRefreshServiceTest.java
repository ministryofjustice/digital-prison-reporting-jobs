package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.common.Util;
import uk.gov.justice.digital.config.ResourceLoader;
import uk.gov.justice.digital.domains.DomainExecutor;
import uk.gov.justice.digital.domains.DomainExecutorTest;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.exceptions.DomainExecutorException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Objects;
import static org.junit.jupiter.api.Assertions.*;

public class DomainRefreshServiceTest extends Util {


    private static TestUtil utils = null;

    @TempDir
    private Path folder;


    @BeforeAll
    public static void setUp() {
        System.out.println("setup method");
        //instantiate and populate the dependencies
        utils = new TestUtil();
    }

    @Test
    public void test_tempFolder() {
        assertNotNull(this.folder);
    }

    protected DomainDefinition getDomain(final String resource) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
        return mapper.readValue(json, DomainDefinition.class);
    }


    @Test
    public void getString_MatchesValuePassedToDomainRefreshService() {
        final String sourcePath = this.folder.toFile().getAbsolutePath()  + "domain/source";
        final String targetPath = this.folder.toFile().getAbsolutePath()  + "domain/target";
        String expectedResult = this.folder.toFile().getAbsolutePath()  + "domain/source";

        DomainRefreshService service = new DomainRefreshService(sourcePath, targetPath, null);
        String result = service.sourcePath;
        assertEquals(expectedResult, result);
    }


    @Test
    public void test_incident_domain() throws IOException {
        final String domainOperation = "insert";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/incident_domain.json");

        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offenders"), df_offenders);

        final Dataset<Row> df_offenderBookings = utils.getOffenderBookings(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offender_bookings"), df_offenderBookings);

        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                System.out.println(Objects.requireNonNull(emptyCheck.list()).length);
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            System.out.println("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch (Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    @Test
    public void test_establishment_domain_insert() throws IOException {
        final String domainOperation = "insert";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                System.out.println(Objects.requireNonNull(emptyCheck.list()).length);
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            System.out.println("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch (Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    @Test
    public void test_establishment_domain_update() throws IOException {
        final String domainOperation = "update";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') update started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                System.out.println(Objects.requireNonNull(emptyCheck.list()).length);
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            System.out.println("DomainRefresh::process('" + domain.getName() + "') update completed");
        } catch (Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    @Test
    public void test_establishment_domain_delete() throws IOException {
        final String domainOperation = "delete";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') delete started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                System.out.println(Objects.requireNonNull(emptyCheck.list()).length);
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            System.out.println("DomainRefresh::process('" + domain.getName() + "') delete completed");
        } catch (Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }


    @Test
    public void test_handle_error() {
        try {
            throw new DomainExecutorException("test message");
        } catch (DomainExecutorException e){
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.err.print(sw.getBuffer().toString());
            assertTrue(true);
        } finally {
            System.out.println("Test Completed");
        }

    }
}
