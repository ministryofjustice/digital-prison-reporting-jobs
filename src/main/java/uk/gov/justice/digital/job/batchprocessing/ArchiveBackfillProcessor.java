package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.BackfillException;
import uk.gov.justice.digital.service.DataStorageService;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.common.CommonDataFields.DEFAULT_VALUE_KEY;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Singleton
public class ArchiveBackfillProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveBackfillProcessor.class);
    private final DataStorageService storageService;

    @Inject
    public ArchiveBackfillProcessor(DataStorageService storageService) {
        this.storageService = storageService;
    }


    public void createBackfilledArchiveData(SourceReference sourceReference, String outputBasePath, Dataset<Row> archiveDataset) {
        String source = sourceReference.getSource();
        String table = sourceReference.getTable();
        logger.debug("Back-filling archive records with nullable columns for table {}.{}", source, table);
        final Dataset<Row> backfilledArchiveRecords = backfillNullableColumns(sourceReference, archiveDataset);

        logger.debug("Writing back-filled archive records for table {}.{}", source, table);
        String backfillOutputPath = createValidatedPath(outputBasePath, source, table);

        if (backfilledArchiveRecords.isEmpty()) {
            throw new BackfillException("Computed back-fill data is empty for table " + source + "." + table);
        } else {
            storageService.overwriteParquet(backfillOutputPath, backfilledArchiveRecords);
        }
    }

    private static Dataset<Row> backfillNullableColumns(SourceReference sourceReference, Dataset<Row> archiveDataset) {
        val nonNullableFields = Arrays.stream(sourceReference.getSchema().fields())
                .filter(field -> !field.nullable())
                .toList();

        val nonNullableColumns = nonNullableFields
                .stream()
                .filter(field -> !field.metadata().contains(DEFAULT_VALUE_KEY))
                .collect(Collectors.toMap(StructField::name, StructField::dataType));

        val nullableColumns = Arrays.stream(sourceReference.getSchema().fields())
                .filter(StructField::nullable)
                .collect(Collectors.toMap(StructField::name, StructField::dataType));

        val archiveDatasetColumns = Arrays.stream(archiveDataset.schema().fields())
                .collect(Collectors.toMap(StructField::name, StructField::dataType));

        val violatesNonNullableColumnRule = nonNullableColumns.entrySet().stream().anyMatch(column -> !archiveDatasetColumns.containsKey(column.getKey()));
        if (violatesNonNullableColumnRule) {
            String source = sourceReference.getSource();
            String table = sourceReference.getTable();
            throw new BackfillException("Mandatory column(s) in schema does not exist in archived data for table " + source + "." + table);
        }

        Map<String, Column> defaultColumnsMap = nonNullableFields
                .stream()
                .filter(field -> field.metadata().contains(DEFAULT_VALUE_KEY))
                .collect(Collectors.toMap(StructField::name, column -> lit(column.metadata().getString(DEFAULT_VALUE_KEY)).cast(column.dataType())));

        archiveDatasetColumns.keySet().forEach(defaultColumnsMap::remove);

        nullableColumns.keySet().removeAll(archiveDatasetColumns.keySet());

        Map<String, Column> nullableColumnMap = nullableColumns
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, column -> lit(null).cast(column.getValue())));

        return archiveDataset
                .withColumns(nullableColumnMap)
                .withColumns(defaultColumnsMap);
    }
}

