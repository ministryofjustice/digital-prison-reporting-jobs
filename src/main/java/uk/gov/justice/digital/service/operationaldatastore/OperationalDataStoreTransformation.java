package uk.gov.justice.digital.service.operationaldatastore;

import jakarta.inject.Singleton;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

/**
 * Transforms a DataFrame to the format we want to store in the Operational DataStore
 */
@Singleton
public class OperationalDataStoreTransformation {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreTransformation.class);

    Dataset<Row> transform(Dataset<Row> dataFrame) {
        logger.debug("Setting up data transformations ready for Operational DataStore");
        // We normalise columns to lower case to avoid having to quote every column due to Postgres lower casing everything in incoming queries
        Dataset<Row> lowerCaseColsDf = normaliseColumnsToLowerCase(dataFrame);
        // Handle 0x00 null String character which cannot be inserted in to a Postgres text column
        Dataset<Row> withoutNullsDf = stripNullStrings(lowerCaseColsDf);
        // We don't store these metadata columns in the destination table so we remove them
        return withoutNullsDf.drop(OPERATION.toLowerCase(), TIMESTAMP.toLowerCase());
    }

    private static Dataset<Row> normaliseColumnsToLowerCase(Dataset<Row> dataFrame) {
        Column[] lowerCaseCols = Arrays.stream(dataFrame.columns()).map(colName -> col(colName).as(colName.toLowerCase())).toArray(Column[]::new);
        return dataFrame.select(lowerCaseCols);
    }

    private static Dataset<Row> stripNullStrings(Dataset<Row> dataFrame) {
        Dataset<Row> result = dataFrame;
        for (StructField field : dataFrame.schema().fields()) {
            if (field.dataType() instanceof StringType) {
                String columnname = field.name().toLowerCase();
                result = result.withColumn(columnname, regexp_replace(result.col(columnname), "\u0000", ""));
            }
        }
        return result;
    }
}
