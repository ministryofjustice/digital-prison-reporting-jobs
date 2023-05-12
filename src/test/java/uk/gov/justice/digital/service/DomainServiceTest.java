package uk.gov.justice.digital.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.test.BaseSparkTest;
import uk.gov.justice.digital.test.ResourceLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.GenerateTableData.DB_NAME;
import static uk.gov.justice.digital.test.GenerateTableData.TABLE_DATA_PATH;

public class DomainServiceTest extends BaseSparkTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

    private static final DomainSchemaService schemaService = mock(DomainSchemaService.class);
    private static final DataStorageService storage = new DataStorageService();

    private DomainExecutor executor;

    @TempDir
    private Path folder;

    @BeforeAll
    public static void setupCommonMocks() {
        System.out.println("DomainServiceTest beforeAll() start");
        when(schemaService.databaseExists(any())).thenReturn(true);
        when(schemaService.tableExists(any(), any())).thenReturn(true);
        System.out.println("DomainServiceTest beforeAll() done");
    }

    @BeforeEach
    public void setup() {
        System.out.println("DomainServiceTest beforeEach() start");
        val mockJobParameters = mock(JobParameters.class);
        when(mockJobParameters.getCuratedS3Path()).thenReturn(TABLE_DATA_PATH);
        when(mockJobParameters.getDomainTargetPath()).thenReturn(targetPath());
        when(mockJobParameters.getCatalogDatabase()).thenReturn(Optional.of(DB_NAME));
        executor = new DomainExecutor(mockJobParameters, storage, schemaService, sparkSessionProvider);
        System.out.println("DomainServiceTest beforeEach() done");
    }

    @Test
    public void test_incident_domain() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "demographics";
        val domain = getDomain("/sample/domain/incident_domain.json");

        System.out.println("test_incident_domain - doFullDomainRefresh start");
        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        System.out.println("test_incident_domain - doFullDomainRefresh end");
        assertTargetDirectoryNotEmpty();
        System.out.println("test_incident_domain end");
    }

    @Test
    public void test_establishment_domain_insert() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "establishment";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        assertTargetDirectoryNotEmpty();
    }

    @Test
    public void test_living_unit_domain_insert() throws IOException {
        val domainOperation = "insert";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
        assertTargetDirectoryNotEmpty();
    }

    @Test
    public void test_living_unit_domain_update() throws IOException {
        val domainOperation = "update";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
    }

    @Test
    public void test_establishment_domain_delete() throws IOException {
        val domainOperation = "delete";
        val domainTableName = "living_unit";
        val domain = getDomain("/sample/domain/establishment.domain.json");

        executor.doFullDomainRefresh(domain, domain.getName(), domainTableName, domainOperation);
    }

    private DomainDefinition getDomain(String resource) throws JsonProcessingException {
        return mapper.readValue(ResourceLoader.getResource(resource), DomainDefinition.class);
    }

    private void assertTargetDirectoryNotEmpty() {
        assertTrue(Objects.requireNonNull(new File(targetPath()).list()).length > 0);
    }

    private String targetPath() {
        return folder.toFile().getAbsolutePath() + "/target";
    }

}
