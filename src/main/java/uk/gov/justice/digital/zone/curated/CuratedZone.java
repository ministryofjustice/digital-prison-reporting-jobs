package uk.gov.justice.digital.zone.curated;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;

import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

public abstract class CuratedZone implements Zone {
    
    private final String curatedPath;
    private final Writer writer;

    @Inject
    protected CuratedZone(JobArguments jobArguments, Writer writer) {
        this.curatedPath = jobArguments.getCuratedS3Path();
        this.writer = writer;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        val curatedTablePath = createValidatedPath(curatedPath, sourceReference.getSource(), sourceReference.getTable());

        writer.writeValidRecords(spark, curatedTablePath, sourceReference.getPrimaryKey(), dataFrame);

        return dataFrame;
    }

}
