package uk.gov.justice.digital.domain;


import lombok.val;
import lombok.var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.common.SourceMapping;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.domain.model.*;
import uk.gov.justice.digital.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.domain.model.TableTuple;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainSchemaException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainSchemaService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class DomainExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DomainExecutor.class);

    private static final List<String> fullRefreshOperations = Arrays.asList("insert", "update", "sync");

    private final String sourceRootPath;
    private final String targetRootPath;
    private final DataStorageService storage;
    private final String hiveDatabaseName;
    private final DomainSchemaService schema;

    @Inject
    public DomainExecutor(
            JobArguments jobParameters,
            DataStorageService storage,
            DomainSchemaService schema
    ) {
        logger.info("Initializing DomainExecutor");
        this.sourceRootPath = jobParameters.getCuratedS3Path();
        this.targetRootPath = jobParameters.getDomainTargetPath();
        this.storage = storage;
        this.hiveDatabaseName = jobParameters.getDomainCatalogDatabaseName();
        this.schema = schema;
        logger.info("DomainExecutor initialization complete");
    }

    protected void insertTable(SparkSession spark, TableIdentifier tableId, Dataset<Row> dataFrame)
            throws DomainExecutorException {
        logger.info("DomainExecutor:: insertTable");
        String tablePath = tableId.toPath();
        logger.info("Domain insert to disk started");
        try {
            if (storage.exists(spark, tableId)) {
                if (storage.hasRecords(spark, tableId)) {
                    // this is trying to overwrite so throw an exception
                    throw new DomainExecutorException("Delta table " + tableId.getTable() + " already exists and has records. Try replace");
                } else {
                    storage.replace(tablePath, dataFrame);
                    logger.warn("Storage is empty so performing a replace instead");
                }
            } else {
                storage.create(tablePath, dataFrame);
            }
            // create the schema
            schema.create(tableId, tablePath, dataFrame);
            logger.info("Creating delta table completed...");

        } catch (DomainSchemaException | DataStorageException dse) {
            logger.error("Delta table already exists and contain records", dse);
            throw new DomainExecutorException(dse);
        }
    }

    protected void updateTable(SparkSession spark, TableIdentifier tableId, Dataset<Row> dataFrame)
            throws DomainExecutorException {
        logger.info("DomainExecutor:: updateTable");
        String tablePath = tableId.toPath();
        try {
            if (storage.exists(spark, tableId)) {
                storage.replace(tablePath, dataFrame);
                schema.replace(tableId, tablePath, dataFrame);
                logger.info("Updating delta table completed");
            } else {
                logger.error("Delta table " + tablePath + " doesn't exist");
                throw new DomainExecutorException("Delta table " + tablePath + " doesn't exist");
            }
        } catch (Exception e) {
            logger.error("Delta table update failed", e);
            throw new DomainExecutorException(e);
        }
    }

    protected void syncTable(SparkSession spark, TableIdentifier tableId, Dataset<Row> dataFrame)
            throws DomainExecutorException, DataStorageException {
        logger.info("DomainExecutor:: syncTable");
        if (storage.exists(spark, tableId)) {
            storage.resync(tableId.toPath(), dataFrame);
            logger.info("Syncing delta table completed..." + tableId.getTable());
        } else {
            logger.error("Delta table " + tableId.getTable() + "doesn't exist");
            throw new DomainExecutorException("Delta table " + tableId.getTable() + "doesn't exist");
        }
    }

    protected void deleteTable(SparkSession spark, TableIdentifier tableId) throws DomainExecutorException {
        logger.info("DomainOperations:: deleteSchemaAndTableData");
        try {
            if (storage.exists(spark, tableId)) {
                storage.delete(spark, tableId);
                storage.endTableUpdates(spark, tableId);
            } else {
                logger.warn("Delete table " + tableId.getSchema() + "." + tableId.getTable() + " not executed as table doesn't exist");
            }

            schema.drop(tableId);
        } catch (DomainSchemaException | DataStorageException dse) {
            logger.error("Delta table delete failed", dse);
            throw new DomainExecutorException(dse);
        }
    }

    protected void saveTable(SparkSession spark, TableIdentifier tableId, Dataset<Row> dataFrame,
                             String domainOperation)
            throws DomainExecutorException, DataStorageException {
        logger.info("DomainOperations::saveTable");
        if (domainOperation.equalsIgnoreCase("insert")) {
            insertTable(spark, tableId, dataFrame);
        } else if (domainOperation.equalsIgnoreCase("update")) {
            updateTable(spark, tableId, dataFrame);
        } else if (domainOperation.equalsIgnoreCase("sync")) {
            syncTable(spark, tableId, dataFrame);
        } else {
            // Handle invalid operation type
            logger.error("Invalid operation type " + domainOperation);
            throw new DomainExecutorException("Invalid operation type " + domainOperation);
        }
        storage.endTableUpdates(spark, tableId);
    }

    private void saveViolations(SparkSession spark, TableIdentifier target, Dataset<Row> dataFrame) throws DataStorageException {
        storage.append(target.toPath(), dataFrame);
        storage.endTableUpdates(spark, target);
    }

    public Dataset<Row> getAllSourcesForTable(
            SparkSession spark,
            String sourcePath,
            String source,
            TableTuple exclude
    ) {
        if (exclude == null || !exclude.asString().equalsIgnoreCase(source)) {
            try {
                TableTuple full = new TableTuple(source);
                Dataset<Row> dataFrame = storage.get(
                        spark,
                        new TableIdentifier(sourcePath, hiveDatabaseName, full.getSchema(), full.getTable())
                );
                if (dataFrame != null) {
                    logger.info("Loaded source '" + full.asString() + "'.");
                    return dataFrame;
                } else {
                    logger.error("Source " + full.asString() + " not found");
                    throw new DomainExecutorException("Source " + full.asString() + " not found");
                }
            } catch (Exception e) {
                logger.error("Unable to get the source information", e);
            }
        } else {
            // we already have this table
            logger.info("Table already present " + exclude.asString());
        }
        return null;
    }


    public Dataset<Row> applyViolations(SparkSession spark, Dataset<Row> dataFrame, List<ViolationDefinition> violations)
            throws DataStorageException {
        var sourceDataframe = dataFrame;
        for (val violation : violations) {
            val applyCondition = violation.getCheck();
            val violationsDataframe = sourceDataframe.where(applyCondition).toDF();
            if (violationsDataframe.isEmpty()) {
                logger.info("No Violation records found for condition " + applyCondition);
            } else {
                logger.info("Removing violation records");
                TableIdentifier info = new TableIdentifier(
                        targetRootPath,
                        hiveDatabaseName,
                        violation.getLocation(),
                        violation.getName()
                );
                saveViolations(spark, info, violationsDataframe);
                sourceDataframe = sourceDataframe.except(violationsDataframe);
            }
        }
        return sourceDataframe;
    }

    public Dataset<Row> applyTransform(SparkSession spark, Map<String, Dataset<Row>> dfs, TransformDefinition transform)
            throws DomainExecutorException {
        List<String> sources = new ArrayList<>();
        String view = transform.getViewText().toLowerCase();
        try {
            if (view.isEmpty()) {
                logger.error("View text is empty");
                throw new DomainExecutorException("View text is empty");
            }

            for (String source : transform.getSources()) {
                String src = source.toLowerCase().replace(".", "__");
                Dataset<Row> sourceDf = dfs.get(source);
                if (sourceDf != null) {
                    sourceDf.createOrReplaceTempView(src);
                    logger.info("Added view '" + src + "'");
                    sources.add(src);
                    if (schemaContains(sourceDf, "_operation") &&
                            schemaContains(sourceDf, "_timestamp")) {
                        view = view.replace(" from ", ", " + src + "._operation, " + src + "._timestamp from ");
                    }
                }
                logger.info(view);
                view = view.replace(source, src);
            }
            logger.info("Executing view '" + view + "'...");
            // This will validate whether the given SQL is valid
            return validateSQLAndExecute(spark, view);
        } finally {
            if (!view.isEmpty()) {
                sources.forEach(s -> spark.catalog().dropTempView(s));
            }
        }
    }

    public void applyDomain(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String domainName,
            TableDefinition tableDefinition,
            DMS_3_4_7.Operation operation
    ) throws DataStorageException {
        val primaryKeyName = tableDefinition.getPrimaryKey();
        val domainTableName = tableDefinition.getName();

        logger.debug("Writing {} record: {} with primary key {}", operation.getName(), domainName, primaryKeyName);

        TableIdentifier target = new TableIdentifier(
                targetRootPath,
                hiveDatabaseName,
                domainName,
                domainTableName
        );
        val primaryKey = new SourceReference.PrimaryKey(primaryKeyName);
        switch (operation) {
            case Delete:
                storage.deleteRecords(spark, target.toPath(), dataFrame, primaryKey);
                storage.endTableUpdates(spark, target);
                break;
            case Insert:
                storage.upsertRecords(spark, target.toPath(), dataFrame, primaryKey);
                storage.endTableUpdates(spark, target);
                break;
            case Update:
                storage.updateRecords(spark, target.toPath(), dataFrame, primaryKey);
                storage.endTableUpdates(spark, target);
                break;
            default:
                break;
        }
    }

    private Dataset<Row> validateSQLAndExecute(SparkSession spark, String query) {
        Dataset<Row> dataframe = null;
        if (!query.isEmpty()) {
            try {
                spark.sql(query).queryExecution().explainString(ExplainMode.fromString("simple"));
                dataframe = spark.sqlContext().sql(query).toDF();
            } catch (Exception e) {
                logger.error("SQL text is invalid: " + query, e);
            }
        }
        return dataframe;
    }


    // Mapping will be enhanced at later stage in future user stories
    protected Dataset<Row> applyMappings(Dataset<Row> dataFrame, TableDefinition.MappingDefinition mapping) {
        if (mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()
                && !dataFrame.isEmpty()) {
            return dataFrame.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return dataFrame;
    }

    public Dataset<Row> apply(SparkSession spark, TableDefinition table, Map<String, Dataset<Row>> sourceTableMap)
            throws DomainExecutorException {
        try {
            logger.info("Apply Method for " + table.getName() + "...");
            // Transform
            Map<String, Dataset<Row>> refs = new HashMap<>();
            // Add sourceTable if present
            if (sourceTableMap != null && sourceTableMap.size() > 0) {
                refs.putAll(sourceTableMap);
            } else if (table.getTransform() != null && table.getTransform().getSources() != null
                    && !table.getTransform().getSources().isEmpty()) {
                for (String source : table.getTransform().getSources()) {
                    Dataset<Row> sourceDataFrame = getAllSourcesForTable(spark, sourceRootPath, source, null);
                    if (sourceDataFrame != null) {
                        refs.put(source.toLowerCase(), sourceDataFrame);
                    } else {
                        logger.info("Unable to load source '" + source + "' for Table Definition '" + table.getName() + "'");
                        throw new DomainExecutorException("Unable to load source '" + source +
                                "' for Table Definition '" + table.getName() + "'");
                    }
                }
            } else {
                // Expecting this condition should never be reached
                logger.info("TableDefinition is invalid");
            }


            logger.info("'" + table.getName() + "' has " + refs.size() + " references to tables...");
            Dataset<Row> transformedDataFrame = applyTransform(spark, refs, table.getTransform());

            logger.info("Apply Violations for " + table.getName() + "...");
            // Process Violations - we now have a subset
            Dataset<Row> postViolationsDataFrame = applyViolations(spark, transformedDataFrame, table.getViolations());

            logger.info("Apply Mappings for " + table.getName() + "...");
            // Mappings
            return applyMappings(postViolationsDataFrame, table.getMapping());

        } catch (Exception e) {
            logger.info("Apply Domain for " + table.getName() + " failed.");
            throw new DomainExecutorException("Apply Domain for " + table.getName() + " failed :", e);
        } finally {
            logger.info("Apply Domain process for " + table.getName() + " completed.");
        }
    }

    public void doDomainDelete(SparkSession spark, String domainName, String domainTableName) throws DomainExecutorException {
        val tableId = new TableIdentifier(targetRootPath, hiveDatabaseName, domainName, domainTableName);
        logger.info("Executing delete for {}", tableId);
        deleteTable(spark, tableId);
    }

    public void doFullDomainRefresh(
            SparkSession spark,
            DomainDefinition domainDefinition,
            String domainTableName,
            String operation
    ) throws DomainExecutorException {

        if (fullRefreshOperations.contains(operation.toLowerCase())) {
            val table = domainDefinition.getTables().stream()
                    .filter(t -> domainTableName.equals(t.getName()))
                    .findAny()
                    .orElseThrow(() -> new DomainExecutorException(
                            "Table '" + domainTableName +
                                    "' not present in definition for domain: " +
                                    domainDefinition.getName()));


            // no source table and df they are required only for unit testing
            val dfTarget = apply(spark, table, null);

            try {
                saveTable(
                        spark,
                        new TableIdentifier(
                                targetRootPath,
                                hiveDatabaseName,
                                domainDefinition.getName(),
                                table.getName()
                        ),
                        dfTarget,
                        operation
                );
            } catch (DataStorageException e) {
                logger.error("Saving domain data failed", e);
                throw new DomainExecutorException(e.getMessage(), e);
            }

        } else {
            val message = "Unsupported domain operation: '" + operation + "'";
            throw new DomainExecutorException(message);
        }
    }

    public Dataset<Row> getAdjoiningDataFrame(
            SparkSession spark,
            SourceMapping sourceMapping,
            Dataset<Row> referenceDataFrame
    ) {
        val adjoiningSourceName = sourceMapping.getDestinationTable();
        val tableTuple = new TableTuple(adjoiningSourceName);
        val tableIdentifier = new TableIdentifier(sourceRootPath, hiveDatabaseName, tableTuple.getSchema(), tableTuple.getTable());

        val columnMappings = sourceMapping.getColumnMap();
        val columnTypes = new HashMap<String, String>();
        Arrays.stream(referenceDataFrame.schema().fields()).forEach(field -> columnTypes.put(field.name(), field.dataType().typeName()));

        if (columnMappings.isEmpty()) {
            return spark.emptyDataFrame();
        } else {
            val conjunctiveOperator = sourceMapping.getTableAliases().isEmpty()? " and " : " or ";
            // Create a filter expression using the column names and values from the reference dataFrame
            val filterExpression = columnMappings.keySet().stream().map(columnMapping -> {
                        val adjoiningTableColumnName = columnMappings.get(columnMapping).getColumnName();
                        val referenceSourceColumnName = columnMapping.getColumnName();
                        val columnTypeName = columnTypes.getOrDefault(referenceSourceColumnName, DataTypes.NullType.typeName());

                        if (isNumericType(columnTypeName) || columnTypeName.equalsIgnoreCase(DataTypes.BooleanType.typeName())) {
                            val columnValue = referenceDataFrame.first().getAs(referenceSourceColumnName);
                            return adjoiningTableColumnName + " = " + columnValue;
                        } else {
                            String columnValue = referenceDataFrame.first().getAs(referenceSourceColumnName);
                            // Adding quotes to non-numeric and non-boolean column values
                            return adjoiningTableColumnName + " = '" + columnValue + "'";
                        }
                    }
            ).collect(Collectors.joining(conjunctiveOperator));

            logger.debug("Adjoining dataframe {} selection expression: {}", adjoiningSourceName, filterExpression);

            val adjoiningDataFrame = storage.get(spark, tableIdentifier);
            // Filter rows in source dataFrame using the filter expression
            if (adjoiningDataFrame.isEmpty()) {
                logger.warn("Adjoining dataFrame from path {} is empty", tableIdentifier.toPath());
                return adjoiningDataFrame;
            } else {
                return adjoiningDataFrame.where(filterExpression);
            }
        }
    }

    private static boolean isNumericType(String columnTypeName) {
        return columnTypeName.equalsIgnoreCase(DataTypes.LongType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.ShortType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.IntegerType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.FloatType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.DoubleType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.ByteType.typeName()) ||
                columnTypeName.equalsIgnoreCase(DataTypes.createDecimalType().typeName());
    }

    private boolean schemaContains(Dataset<Row> dataFrame, String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }
}