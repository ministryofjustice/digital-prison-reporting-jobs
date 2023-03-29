package uk.gov.justice.digital.converter.dms;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DMS_3_4_6Test {

    private static SparkSession sparkSession;

    @BeforeAll
    public static void beforeAll(){
        sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/data/dms_data.json"
    })
    @DisplayName("Test Raw DMS 3.4.6 data.")
    public void testDMSConvertProcessForValidData(final String rawDataPath) {
        Dataset<Row> df = sparkSession.read().json(rawDataPath);

        DMS_3_4_6 dms_3_4_6 = new DMS_3_4_6();
        Dataset<Row> resultDF = dms_3_4_6.convert(df.javaRDD(), sparkSession);

        assertEquals(df.count(), resultDF.count());
    }


    @AfterAll
    public static void afterAll(){
        sparkSession.stop();
    }

}
