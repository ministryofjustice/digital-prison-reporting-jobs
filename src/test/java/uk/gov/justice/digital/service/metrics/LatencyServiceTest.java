package uk.gov.justice.digital.service.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.SparkTestBase;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.metrics.LatencyStatistics.isEmpty;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

class LatencyServiceTest extends SparkTestBase {

    private final LatencyService underTest = new LatencyService();

    @Test
    void shouldCalculateStats() {
        long nowMillis = toMillis("2023-11-13 10:52:55.000000");


        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.000000", Insert, "1"),
                createRow(2, "2023-11-13 10:50:05.000000", Insert, "2"),
                createRow(3, "2023-11-13 10:52:30.000000", Insert, "3")
        ), TEST_DATA_SCHEMA);

        LatencyStatistics result = underTest.calculateLatencyStatistics(df, TIMESTAMP, nowMillis);
        assertEquals(25000L, result.getMinimum());
        assertEquals(175000L, result.getMaximum());
        assertEquals(370000L, result.getSum());
        assertEquals(3L, result.getTotalCount());
    }

    @Test
    void shouldReturnEmptyStatisticsForEmptyDataFrame() {
        long nowMillis = toMillis("2023-11-13 10:52:55.000000");

        Dataset<Row> emptyDf = spark.createDataFrame(List.of(), TEST_DATA_SCHEMA);

        LatencyStatistics result = underTest.calculateLatencyStatistics(emptyDf, TIMESTAMP, nowMillis);
        assertTrue(isEmpty(result));
    }

    @Test
    void shouldReturnEmptyStatisticsForAllNullTimestamps() {
        long nowMillis = toMillis("2023-11-13 10:52:55.000000");


        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                createRow(1, null, Insert, "1"),
                createRow(2, null, Insert, "2"),
                createRow(3, null, Insert, "3")
        ), TEST_DATA_SCHEMA);

        LatencyStatistics result = underTest.calculateLatencyStatistics(df, TIMESTAMP, nowMillis);
        assertTrue(isEmpty(result));
    }

    @Test
    void shouldFilterOutNullTimestampsForMixedNullAndNonNullTimestamps() {
        long nowMillis = toMillis("2023-11-13 10:52:55.000000");


        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                createRow(1, null, Insert, "1"),
                createRow(2, null, Insert, "2"),
                createRow(3, "2023-11-13 10:52:30.000000", Insert, "3")
        ), TEST_DATA_SCHEMA);

        LatencyStatistics result = underTest.calculateLatencyStatistics(df, TIMESTAMP, nowMillis);
        assertEquals(25000L, result.getMinimum());
        assertEquals(25000L, result.getMaximum());
        assertEquals(25000L, result.getSum());
        assertEquals(1L, result.getTotalCount());
    }

    private long toMillis(String timestampStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        LocalDateTime dateTime = LocalDateTime.parse(timestampStr, formatter);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
