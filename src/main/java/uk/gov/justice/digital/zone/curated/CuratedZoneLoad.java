package uk.gov.justice.digital.zone.curated;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.curated.CuratedZoneLoadWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TABLE;

@Singleton
public class CuratedZoneLoad extends CuratedZone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneLoad.class);

    @Inject
    public CuratedZoneLoad(
            JobArguments jobArguments,
            DataStorageService storage,
            SourceReferenceService sourceReferenceService
    ) {
        this(jobArguments, sourceReferenceService, createWriter(storage));
    }

    private CuratedZoneLoad(
            JobArguments jobArguments,
            SourceReferenceService sourceReference,
            Writer writer
    ) {
        super(jobArguments, sourceReference, writer);
    }

    private static Writer createWriter(DataStorageService storage) {
        return new CuratedZoneLoadWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        if (dataFrame.isEmpty()) {
            return spark.emptyDataFrame();
        } else {
            val rowCount = dataFrame.count();
            String sourceName = table.getAs(SOURCE);
            String tableName = table.getAs(TABLE);

            val startTime = System.currentTimeMillis();

            logger.warn("Processing {} records for {}/{}", rowCount, sourceName, tableName);
            val result = super.process(spark, dataFrame, table);
            logger.warn("Processed batch with {} rows in {}ms", rowCount, System.currentTimeMillis() - startTime);

            return result;
        }
    }

}
