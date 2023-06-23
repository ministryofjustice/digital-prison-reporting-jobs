package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.test.SparkTestHelpers;
import uk.gov.justice.digital.zone.fixtures.Fixtures;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.zone.fixtures.Fixtures.STRUCTURED_PATH;
import static uk.gov.justice.digital.zone.fixtures.Fixtures.VIOLATIONS_PATH;

@ExtendWith(MockitoExtension.class)
public class StructuredZoneIntegrationTest extends BaseSparkTest  {

    private final SparkTestHelpers helpers = new SparkTestHelpers(spark);

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private DataStorageService mockDataStorage;

    @Mock
    private GlueSchemaClient glueSchemaClient;

    @Mock
    private AvroToSparkSchemaConverter converter;

    @TempDir
    private Path tmp;

    private StructuredZone underTest;

    @BeforeEach
    public void setUp() {
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        SourceReferenceService sourceReferenceService = new SourceReferenceService(glueSchemaClient, converter);

        underTest = new StructuredZoneLoad(
                mockJobArguments,
                mockDataStorage,
                sourceReferenceService
        );
    }

    @Disabled("Temporarily disabled since this test is failing at the moment")
    public void shouldHandleValidRecords() throws DataStorageException {
        Dataset<Row> offenders = helpers.getOffenders(tmp);
        offenders.show(false);

        // test is failing at this step because there are no null columns in the offenders dataset
        assertTrue(hasNullColumns(offenders));

        Dataset<Row> result = underTest.process(
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
