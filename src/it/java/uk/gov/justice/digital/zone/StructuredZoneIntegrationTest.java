package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.test.SparkTestHelpers;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.zone.Fixtures.*;
import static uk.gov.justice.digital.zone.Fixtures.TABLE_NAME;


public class StructuredZoneIntegrationTest extends BaseSparkTest {
    private SparkTestHelpers helpers = new SparkTestHelpers(spark);
    private final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private DataStorageService mockDataStorage;


    private SourceReferenceService sourceReferenceService;

    @Mock
    private GlueSchemaClient glueSchemaClient;

    @Mock
    private AvroToSparkSchemaConverter converter;

    @TempDir
    private Path tmp;

    private StructuredZone underTest;

    private AutoCloseable closeable;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        sourceReferenceService = new SourceReferenceService(glueSchemaClient, converter);

        underTest = new StructuredZone(
                mockJobArguments,
                mockDataStorage,
                sourceReferenceService
        );
    }

    @AfterEach
    public void afterEach() throws Exception {
        closeable.close();
    }

    @Test
    public void shouldHandleValidRecords() throws DataStorageException {
        Dataset<Row> offenders = helpers.getOffenders(tmp);
        offenders.show(false);

        assertTrue(hasNullColumns(offenders));

        Dataset<Row> result = underTest.processLoad(
                spark,
                offenders,
                new GenericRowWithSchema(new Object[] { "oms_owner", "offenders", "load" }, Fixtures.ROW_SCHEMA)
        );

        assertTrue(hasNullColumns(result));
    }

    private boolean hasNullColumns(Dataset<Row> df) {
        for(String c: df.columns()) {
            if(df.filter(col(c).isNull()).count() > 0)
                return true;
        }
        return false;
    }


}
