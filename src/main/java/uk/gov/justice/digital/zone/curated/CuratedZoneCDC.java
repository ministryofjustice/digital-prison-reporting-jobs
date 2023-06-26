package uk.gov.justice.digital.zone.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.curated.CuratedZoneCdcWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CuratedZoneCDC extends CuratedZone {

    @Inject
    public CuratedZoneCDC(
            JobArguments jobArguments,
            DataStorageService storage,
            SourceReferenceService sourceReferenceService
    ) {
        this(jobArguments, sourceReferenceService, createWriter(storage));
    }

    private CuratedZoneCDC(
            JobArguments jobArguments,
            SourceReferenceService sourceReference,
            Writer writer
    ) {
        super(jobArguments, sourceReference, writer);
    }

    private static Writer createWriter(DataStorageService storage) {
        return new CuratedZoneCdcWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        return super.process(spark, dataFrame, table);
    }

}
