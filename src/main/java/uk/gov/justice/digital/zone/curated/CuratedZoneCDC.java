package uk.gov.justice.digital.zone.curated;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.curated.CuratedZoneCdcWriter;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public class CuratedZoneCDC extends CuratedZone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneCDC.class);

    @Inject
    public CuratedZoneCDC(
            JobArguments jobArguments,
            DataStorageService storage
    ) {
        this(jobArguments, createWriter(storage));
    }

    @SuppressWarnings("unused")
    private CuratedZoneCDC(
            JobArguments jobArguments,
            Writer writer
    ) {
        super(jobArguments, writer);
    }

    private static Writer createWriter(DataStorageService storage) {
        return new CuratedZoneCdcWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        if (dataFrame.isEmpty()) {
            return spark.emptyDataFrame();
        } else {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            val startTime = System.currentTimeMillis();

            logger.debug("Processing records for {}/{}", sourceName, tableName);
            val result = super.process(spark, dataFrame, sourceReference);
            logger.debug("Processed batch in {}ms", System.currentTimeMillis() - startTime);

            return result;
        }
    }

}
