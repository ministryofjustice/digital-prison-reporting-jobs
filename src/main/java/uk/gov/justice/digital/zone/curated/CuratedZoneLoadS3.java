package uk.gov.justice.digital.zone.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.ZoneLoad;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CuratedZoneLoadS3 {

    private final ZoneLoad internalZoneLoad;

    @Inject
    public CuratedZoneLoadS3(
            JobArguments jobArguments,
            DataStorageService storage,
            ViolationService violationService
    ) {
        this.internalZoneLoad = new ZoneLoad(storage, violationService, jobArguments.getCuratedS3Path(), ViolationService.ZoneName.CURATED_LOAD);
    }
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        return internalZoneLoad.process(spark, dataFrame, sourceReference);
    }
}
