package uk.gov.justice.digital.zone.operational;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.glue.GlueClientProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.Arrays;

import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

@ExtendWith(MockitoExtension.class)
public class TempTest extends BaseSparkTest {

    @Mock
    private SourceReference sourceReference;

    @Test
    public void testRecordByRecord() {
        OperationalZoneCDCRecordByRecord underTest = new OperationalZoneCDCRecordByRecord(new GlueClient(new GlueClientProvider()));
        StructType schemaWithoutMetadataFields = new StructType(new StructField[]{
                new StructField("pk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("data", DataTypes.StringType, true, Metadata.empty())
        });

        StructType testDataSchema = new StructType(new StructField[]{
                new StructField("pk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("data", DataTypes.StringType, true, Metadata.empty()),
                new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
                new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "b", "I", ""),
                RowFactory.create(2, "b", "U", ""),
                RowFactory.create(3, "b", "D", "")
        ), testDataSchema);

        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("pk"));
        when(sourceReference.getSource()).thenReturn("public");
        when(sourceReference.getTable()).thenReturn("my_table");
        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataFields);

        underTest.process(spark, df, sourceReference);
    }

    @Test
    public void testBulk() {
        OperationalZoneCDCBulk underTest = new OperationalZoneCDCBulk(new GlueClient(new GlueClientProvider()));
        StructType schemaWithoutMetadataFields = new StructType(new StructField[]{
                new StructField("pk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("data", DataTypes.StringType, true, Metadata.empty())
        });

        StructType testDataSchema = new StructType(new StructField[]{
                new StructField("pk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("data", DataTypes.StringType, true, Metadata.empty()),
                new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
                new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "b", "I", ""),
                RowFactory.create(2, "b", "U", ""),
                RowFactory.create(3, "b", "D", "")
        ), testDataSchema);

        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("pk"));
        when(sourceReference.getSource()).thenReturn("public");
        when(sourceReference.getTable()).thenReturn("my_table_bulk");
        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataFields);

        underTest.process(spark, df, sourceReference);
    }
}
