package uk.gov.justice.digital.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainSchemaService;
import uk.gov.justice.digital.test.ResourceLoader;
import uk.gov.justice.digital.test.SparkTestHelpers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class DomainExecutorTest extends BaseSparkTest {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected static final SparkTestHelpers helpers = new SparkTestHelpers(spark);
    protected static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
    protected static final DataStorageService storage = new DataStorageService();
    protected static final String hiveDatabaseName = "test_db";

    @TempDir
    protected Path tmp;

    protected static final String SAMPLE_EVENTS_PATH =
            Objects.requireNonNull(
                    DomainExecutorTest.class.getResource("/sample/events")
            ).getPath();

    @Mock
    protected Dataset<Row> mockedDataSet;

    protected final DataStorageService testStorage = mock(DataStorageService.class);

    protected DomainDefinition getDomain(String resource) throws IOException {
        return mapper.readValue(ResourceLoader.getResource(resource), DomainDefinition.class);
    }

    protected boolean areEqual(Dataset<Row> a, Dataset<Row> b) {
        return a.schema().equals(b.schema()) &&
                Arrays.equals(a.collectAsList().toArray(), b.collectAsList().toArray());
    }

    protected DomainExecutor createExecutor(String source, String target, DataStorageService storage,
                                          DomainSchemaService schemaService) {
        val mockJobParameters = mock(JobArguments.class);
        when(mockJobParameters.getCuratedS3Path()).thenReturn(source);
        when(mockJobParameters.getDomainTargetPath()).thenReturn(target);
        when(mockJobParameters.getDomainCatalogDatabaseName()).thenReturn(hiveDatabaseName);
        return new DomainExecutor(mockJobParameters, storage, schemaService, sparkSessionProvider);
    }

    protected String domainTargetPath() {
        return tmp.toFile().getAbsolutePath() + "/domain/target";
    }

    protected String targetPath() {
        return tmp.toFile().getAbsolutePath() + "/target";
    }

    protected String sourcePath() {
        return tmp.toFile().getAbsolutePath() + "/source";
    }

    protected Map<String, Dataset<Row>> getOffenderRefs() {
        val refs = new HashMap<String, Dataset<Row>>();
        refs.put("nomis.offender_bookings", helpers.getOffenderBookings(tmp));
        refs.put("nomis.offenders", helpers.getOffenders(tmp));
        return refs;
    }

}
