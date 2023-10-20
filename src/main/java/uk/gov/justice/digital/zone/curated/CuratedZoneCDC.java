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
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.curated.CuratedZoneCdcWriter;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public class CuratedZoneCDC extends CuratedZone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneCDC.class);

    private final ViolationService violationService;

    @Inject
    public CuratedZoneCDC(
            JobArguments jobArguments,
            DataStorageService storage,
            ViolationService violationService
    ) {
        this(jobArguments, createWriter(storage), violationService);
    }

    @SuppressWarnings("unused")
    private CuratedZoneCDC(
            JobArguments jobArguments,
            Writer writer,
            ViolationService violationService
    ) {
        super(jobArguments, writer);
        this.violationService = violationService;
    }

    private static Writer createWriter(DataStorageService storage) {
        return new CuratedZoneCdcWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        Dataset<Row> result;
        try {
            if (dataFrame.isEmpty()) {
                result = spark.emptyDataFrame();
            } else {
                String sourceName = sourceReference.getSource();
                String tableName = sourceReference.getTable();

                val startTime = System.currentTimeMillis();

                logger.debug("Processing records for {}/{}", sourceName, tableName);
                result = super.process(spark, dataFrame, sourceReference);
                logger.debug("Processed batch in {}ms", System.currentTimeMillis() - startTime);
            }
        } catch (DataStorageRetriesExhaustedException e) {
            logger.warn("Curated zone CDC retries exhausted", e);
            violationService.handleRetriesExhausted(spark, dataFrame, sourceReference.getSource(), sourceReference.getTable(), e, ViolationService.ZoneName.CURATED_CDC);
            result = spark.emptyDataFrame();
        }
        return result;
    }

}
