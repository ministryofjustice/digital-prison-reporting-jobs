package uk.gov.justice.digital.test;

import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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

    public Dataset<Row> getOffenders(Path folder) {
        return loadParquetDataframe(OFFENDERS_SAMPLE_PARQUET_PATH,
                folder.toFile().getAbsolutePath() + "offenders.parquet");

    }

    public Dataset<Row> getOffenderBookings(Path folder) {
        return loadParquetDataframe(OFFENDER_BOOKINGS_SAMPLE_PARQUET_PATH,
                folder.toFile().getAbsolutePath() + "offender-bookings.parquet");
    }

    public Dataset<Row> getInternalAgencyLocations(Path folder) {
        return loadParquetDataframe(INTERNAL_LOCATIONS_SAMPLE_PARQUET_PATH,
                folder.toFile().getAbsolutePath() + "sample-nomis.agency_internal_locations.parquet");
    }

    public Dataset<Row> getAgencyLocations(Path folder) {
        return loadParquetDataframe(AGENCY_LOCATIONS_SAMPLE_PARQUET_PATH,
                folder.toFile().getAbsolutePath() + "sample-nomis.agency_locations.parquet");
    }

    public void persistDataset(TableIdentifier location, Dataset<Row> df) throws DataStorageException {
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", true)
                .option("path", location.toPath())
                .save();
    }

    public Dataset<Row> createIncidentDomainDataframe() {
        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("birth_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("living_unit_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("first_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("last_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("offender_no", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        return spark.read().schema(schema).json(
                spark.emptyDataset(Encoders.STRING()));
    }

    public Dataset<Row> createViolationsDomainDataframe() {
        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("AGE", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("AUDIT_TIMESTAMP", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("BIRTH_DATE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("CREATE_DATE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("CREATE_DATETIME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("CREATE_USER_ID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("FIRST_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ID_SOURCE_CODE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("LAST_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("LAST_NAME_SOUNDEX", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("MIDDLE_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("MODIFY_DATETIME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("NAME_TYPE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("OFFENDER_ID", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("OFFENDER_ID_DISPLAY", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("OFFENDER_NAME_SEQ", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("SEX_CODE", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        return spark.read().schema(schema).json(
                spark.emptyDataset(Encoders.STRING()));
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

    private Dataset<Row> loadParquetDataframe(final String resource, final String filename) {
        try {
            return spark.read().parquet(createFileFromResource(resource, filename).toString());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Path createFileFromResource(final String resource, final String filename) throws IOException {
        val f = Paths.get(filename).toFile();
        FileUtils.copyInputStreamToFile(System.class.getResourceAsStream(resource), f);
        return Paths.get(f.getAbsolutePath());
    }

    public static <T> Seq<T> convertListToSeq(List<T> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

}
