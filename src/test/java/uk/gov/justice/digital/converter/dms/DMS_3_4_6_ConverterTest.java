package uk.gov.justice.digital.converter.dms;

import com.codahale.metrics.MetricRegistryListener;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.BaseSparkTest;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.job.model.Columns.DATA;

public class DMS_3_4_6_ConverterTest extends BaseSparkTest {

    private static final String DATA_PATH = "src/test/resources/data/dms_record.json";

    private static final DMS_3_4_6 underTest = new DMS_3_4_6();


    @Test
    public void shouldConvertValidDataCorrectly() {
        // Load JSON as text and slurp in the context of the whole file so we can read multiline JSON files.
        val rdd = spark
            .read()
            .option("wholetext", "true")
            .text(DATA_PATH)
            .withColumn(DATA, col("value"))
            .javaRDD();

        val converted = underTest.convert(rdd, spark);

        // Strict schema validation is applied so checking accounts agree should be sufficient here.
        assertEquals(rdd.count(), converted.count());
    }

}
