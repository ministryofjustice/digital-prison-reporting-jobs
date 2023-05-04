package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.ResourceLoader;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.DomainExecutorTest;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableInfo;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

public class DomainServiceTest extends BaseSparkTest {

    private static final Logger logger = LoggerFactory.getLogger(DomainServiceTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final SparkTestHelpers utils = new SparkTestHelpers(spark);
    private static final String hiveDatabaseName = "test_db";

    private static final DomainSchemaService schemaService = mock(DomainSchemaService.class);
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
    private static final SparkTestHelpers helpers = new SparkTestHelpers(spark);


    @TempDir
    private Path folder;

    @BeforeAll
    public static void setUp() {
        logger.info("setup method");
        //instantiate and populate the dependencies
        when(schemaService.databaseExists(any())).thenReturn(true);
        when(schemaService.tableExists(any(), any())).thenReturn(true);
    }

    @AfterAll
    public static void tearDown() {
    }

    @Test
    public void test_tempFolder() {
        assertNotNull(this.folder);
    }

    private DomainDefinition getDomain(final String resource) throws IOException {
        val json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
        return mapper.readValue(json, DomainDefinition.class);
    }


    @Test
    public void test_incident_domain() throws IOException {
        final String domainOperation = "insert";
        final String domainTableName = "demographics";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/incident_domain.json");
        final DataStorageService storage = new DataStorageService();

        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "offenders"),
                df_offenders);

        final Dataset<Row> df_offenderBookings = utils.getOffenderBookings(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "offender_bookings"),
                df_offenderBookings);

        try {
            logger.info("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, storage, schemaService,
                    hiveDatabaseName, sparkSessionProvider);
            executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                logger.info(String.valueOf(Objects.requireNonNull(emptyCheck.list()).length));
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            logger.info("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch (Exception e) {
            logger.info("DomainRefresh::process('" + domain.getName() + "') failed");
            fail();
        } finally {
            // Delete the table from Hive
            schemaService.deleteTable(hiveDatabaseName, domain.getName() + "." + domainTableName);
        }
    }

    @Test
    public void test_establishment_domain_insert() throws IOException {
        final String domainOperation = "insert";
        final String domainTableName = "establishment";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");
        final DataStorageService storage = new DataStorageService();

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis",
                        "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            logger.info("Domain Refresh process '" + domain.getName() + "' started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, storage, schemaService,
                    hiveDatabaseName, sparkSessionProvider);
            executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                logger.info(String.valueOf(Objects.requireNonNull(emptyCheck.list()).length));
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            logger.info("Domain Refresh process'" + domain.getName() + "' completed");
        } catch (Exception e) {
            logger.info("Domain Refresh process '" + domain.getName() + "' failed");
            fail();
        } finally {
            // Delete the table from Hive
            schemaService.deleteTable(hiveDatabaseName, domain.getName() + "." + domainTableName);
        }

    }

    @Test
    public void test_living_unit_domain_insert() throws IOException {
        final String domainOperation = "insert";
        final String domainTableName = "living_unit";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");
        final DataStorageService storage = new DataStorageService();

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            logger.info("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, storage, schemaService,
                    hiveDatabaseName, sparkSessionProvider);
            executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                logger.info(String.valueOf(Objects.requireNonNull(emptyCheck.list()).length));
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            logger.info("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch (Exception e) {
            logger.info("DomainRefresh::process('" + domain.getName() + "') failed");
            fail();
        } finally {
            // Delete the table from Hive
            schemaService.deleteTable(hiveDatabaseName, domain.getName() + "." + domainTableName);
        }
    }

    @Test
    public void test_living_unit_domain_update() throws IOException {
        final String domainOperation = "update";
        final String domainTableName = "living_unit";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");
        final DataStorageService storage = new DataStorageService();

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            logger.info("DomainRefresh::process('" + domain.getName() + "') update started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, storage, schemaService,
                    hiveDatabaseName, sparkSessionProvider);
            executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                logger.info(String.valueOf(Objects.requireNonNull(emptyCheck.list()).length));
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            logger.info("DomainRefresh::process('" + domain.getName() + "') update completed");
        } catch (Exception e) {
            logger.info("DomainRefresh::process('" + domain.getName() + "') failed");
            fail();
        } finally {
            // Delete the table from Hive
            schemaService.deleteTable(hiveDatabaseName, domain.getName() + "." + domainTableName);
        }
    }

    @Test
    public void test_establishment_domain_delete() throws IOException {
        final String domainOperation = "delete";
        final String domainTableName = "living_unit";
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/establishment.domain.json");
        final DataStorageService storage = new DataStorageService();

        final Dataset<Row> df_agency_locations = utils.getAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_locations"),
                df_agency_locations);

        final Dataset<Row> df_internal_agency_locations = utils.getInternalAgencyLocations(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, hiveDatabaseName, "nomis", "agency_internal_locations"),
                df_internal_agency_locations);

        try {
            logger.info("DomainRefresh::process('" + domain.getName() + "') delete started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, storage, schemaService,
                    hiveDatabaseName, sparkSessionProvider);
            executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
            File emptyCheck = new File(this.folder.toFile().getAbsolutePath() + "/target");
            if (emptyCheck.isDirectory()) {
                logger.info(String.valueOf(Objects.requireNonNull(emptyCheck.list()).length));
                assertTrue(Objects.requireNonNull(emptyCheck.list()).length > 0);
            }
            logger.info("DomainRefresh::process('" + domain.getName() + "') delete completed");
        } catch (Exception e) {
            logger.info("DomainRefresh::process('" + domain.getName() + "') failed");
            fail();
        }
    }

    @Test
    public void test_hive_create_table()  {
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target/test/table";
        Dataset<Row> df_test = helpers.createSchemaForTest();
        schemaService.createTable(hiveDatabaseName, "test.table", targetPath, df_test);
        schemaService.tableExists(hiveDatabaseName, "test.table");
    }

}
