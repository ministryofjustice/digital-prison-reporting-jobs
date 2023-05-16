package uk.gov.justice.digital.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.test.ResourceLoader;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.test.SparkTestHelpers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DomainServiceTest extends BaseSparkTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String hiveDatabaseName = "test_db";
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
    private static final SparkTestHelpers helpers = new SparkTestHelpers(spark);

    private static final DomainSchemaService schemaService = mock(DomainSchemaService.class);
    private static final DataStorageService storage = new DataStorageService();

    private DomainExecutor executor;

    @TempDir
    private Path folder;

    @BeforeAll
    public static void setupCommonMocks() {
        when(schemaService.databaseExists(any())).thenReturn(true);
        when(schemaService.tableExists(any(), any())).thenReturn(true);
    }

    @BeforeEach
    public void setup() {
        val mockJobParameters = mock(JobArguments.class);
        when(mockJobParameters.getCuratedS3Path()).thenReturn(sourcePath());
        when(mockJobParameters.getDomainTargetPath()).thenReturn(targetPath());
        when(mockJobParameters.getDomainCatalogDatabaseName()).thenReturn(hiveDatabaseName);
        executor = new DomainExecutor(mockJobParameters, storage, schemaService, sparkSessionProvider);
    }

    @Test
    public void test_incident_domain() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "demographics";
        val domain = getDomain("/sample/domain/incident_domain.json");

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "offenders"),
            helpers.getOffenders(folder)
        );

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "offender_bookings"),
            helpers.getOffenderBookings(folder)
        );

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        assertTargetDirectoryNotEmpty();
    }

    @Test
    public void test_establishment_domain_insert() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "establishment";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_locations"),
            helpers.getAgencyLocations(folder)
        );

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_internal_locations"),
            helpers.getInternalAgencyLocations(folder)
        );

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        assertTargetDirectoryNotEmpty();
    }

    @Test
    public void test_living_unit_domain_insert() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_locations"),
            helpers.getAgencyLocations(folder)
        );

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_internal_locations"),
            helpers.getInternalAgencyLocations(folder)
        );

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        assertTargetDirectoryNotEmpty();
    }

    @Test
    public void test_living_unit_domain_update() throws IOException {
        val domainOperation = "update";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_locations"),
            helpers.getAgencyLocations(folder)
        );

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_internal_locations"),
            helpers.getInternalAgencyLocations(folder)
        );

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
    }

    @Test
    public void test_establishment_domain_delete() throws IOException {
        val domainOperation = "delete";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_locations"),
            helpers.getAgencyLocations(folder)
        );

        helpers.persistDataset(
            new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "agency_internal_locations"),
            helpers.getInternalAgencyLocations(folder)
        );

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
    }

    private DomainDefinition getDomain(String resource) throws JsonProcessingException {
        return mapper.readValue(ResourceLoader.getResource(resource), DomainDefinition.class);
    }

    private void assertTargetDirectoryNotEmpty() {
        assertTrue(Objects.requireNonNull(new File(targetPath()).list()).length > 0);
    }

    private String sourcePath() {
        return folder.toFile().getAbsolutePath() + "/source";
    }

    private String targetPath() {
        return folder.toFile().getAbsolutePath() + "/target";
    }

}
