package uk.gov.justice.digital.service.metrics;

import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.unix_millis;

/**
 * Responsible for calculating latency statistics based on timestamps within a Dataframe.
 */
@Singleton
public class LatencyService {

    /**
     * Calculates and returns latency statistics for the provided timestamp column.
     *
     * @param df              - DataFrame containing the data. Must not be empty and must contain the timestampColumn.
     * @param timestampColumn - Column name for which latency statistics are calculated. The column should parse
     *                        to a timestamp using spark's unix_millis(to_timestamp(col(column))) functions. Rows
     *                        with null values for this column will be discarded.
     * @param nowMillis       - Current epoch timestamp in milliseconds.
     * @return LatencyStatistics object containing calculated statistics, which can be empty if latency data cannot
     *                        be calculated due to missing or invalid data.
     */
    public LatencyStatistics calculateLatencyStatistics(Dataset<Row> df, String timestampColumn, long nowMillis) {
        Dataset<Row> withNonNullTimestamps = df.filter(col(timestampColumn).isNotNull());
        long notNullTimestampCount = withNonNullTimestamps.count();
        if (notNullTimestampCount <= 0) {
            return LatencyStatistics.EMPTY;
        }

        String dmsLatencyDiffMsColumn = "dms_latency_diff_ms";

        Dataset<Row> withLatencies = withNonNullTimestamps.withColumn(dmsLatencyDiffMsColumn,
                lit(nowMillis).minus(unix_millis(to_timestamp(col(timestampColumn), "yyyy-MM-dd HH:mm:ss.SSSSSS")))
        );

        withLatencies.show(true);

        Row stats = withLatencies.agg(
                min(dmsLatencyDiffMsColumn).as("Minimum"),
                max(dmsLatencyDiffMsColumn).as("Maximum"),
                sum(dmsLatencyDiffMsColumn).as("Sum")
        ).first();
        Long min = stats.getAs("Minimum");
        Long max = stats.getAs("Maximum");
        Long sum = stats.getAs("Sum");
        return new LatencyStatistics(min, max, sum, notNullTimestampCount);
    }
}
