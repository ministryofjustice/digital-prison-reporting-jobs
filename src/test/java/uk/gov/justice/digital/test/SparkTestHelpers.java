package uk.gov.justice.digital.test;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;


public class SparkTestHelpers {

    public static final String OFFENDERS_SAMPLE_PARQUET_PATH = "/sample/events/nomis/offenders/offenders.parquet";
    public static final String OFFENDER_BOOKINGS_SAMPLE_PARQUET_PATH =
            "/sample/events/nomis/offender_bookings/offender-bookings.parquet";
    public static final String INTERNAL_LOCATIONS_SAMPLE_PARQUET_PATH =
            "/sample/events/nomis/internal-locations/sample-nomis.agency_internal_locations.parquet";
    public static final String AGENCY_LOCATIONS_SAMPLE_PARQUET_PATH =
            "/sample/events/nomis/agency-locations/sample-nomis.agency_locations.parquet";
    private final SparkSession spark;

    public SparkTestHelpers(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> readSampleParquet(String resourcePath) {
        URL resource = System.class.getResource(resourcePath);
        val path = new File(
                resource.getFile()
        ).getAbsolutePath();
        return spark.read().parquet(path);
    }

    public void overwriteDeltaTable(String tablePath, Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .option("overwriteSchema", true)
                .option("path", tablePath)
                .save();
    }

    public static long countParquetFiles(Path path) throws IOException {
        try(Stream<Path> files = Files.list(path)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toFile().getName().endsWith(".parquet"))
                    .count();
        }
    }
}
