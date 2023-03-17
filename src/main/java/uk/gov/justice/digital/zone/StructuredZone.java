package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

@Singleton
public class StructuredZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    // TODO - this duplicates the constants in RawZone
    private static final String LOAD = "load";
    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String OPERATION = "operation";
    private static final String PATH = "path";

    private final String structuredS3Path;

    @Inject
    public StructuredZone(JobParameters jobParameters) {
        this.structuredS3Path = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path now set - unable to create StructuredZone instance"
            ));
    }

    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing data frame with " + dataFrame.count() + " rows");

        val startTime = System.currentTimeMillis();

        uniqueTablesForLoad(dataFrame).forEach((table) -> {
                logger.info("Processing table: {}", table);

                // Locate schema
                String rowSource = table.getAs(SOURCE);
                String rowTable = table.getAs(TABLE);

                logger.info("Locating schema for source: {} table: {}", rowSource, rowTable);

                val schema = SourceReferenceService.getSchema(rowSource, rowTable);

                logger.info("Found schema: {}", schema);

                // Apply schema to DF and log out
                dataFrame.withColumn("parsedData", from_json(col("data"), schema))
                    .select(col("parsedData.*"))
                    .foreach((ForeachFunction<Row>) r -> {
                        logger.info("Processing row: {}", r);
                    });
            });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

    // TODO - duplicated from RawZone
    private List<Row> uniqueTablesForLoad(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).isin(LOAD))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

}
