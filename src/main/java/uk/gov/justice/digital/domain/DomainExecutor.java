package uk.gov.justice.digital.domain;


import lombok.val;
import lombok.var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.model.*;
import uk.gov.justice.digital.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.digital.exception.DomainSchemaException;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import java.util.*;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.service.DomainSchemaService;
import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public class DomainExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DomainExecutor.class);

    private final String sourceRootPath;
    private final String targetRootPath;
    private final DataStorageService storage;
    private final SparkSession spark;
    private final String hiveDatabaseName;
    private final DomainSchemaService schema;

    @Inject
    public DomainExecutor(JobParameters jobParameters,
                          DataStorageService storage,
                          DomainSchemaService schema,
                          SparkSessionProvider sparkSessionProvider
                          ) {

        this.sourceRootPath = jobParameters.getCuratedS3Path();
        this.targetRootPath = jobParameters.getDomainTargetPath();
        this.storage = storage;
        this.hiveDatabaseName = jobParameters.getCatalogDatabase()
            .orElseThrow(() -> new IllegalStateException(
                "Hive Catalog database not set - unable to create Hive Catalog schema"
            ));
        this.schema = schema;
        this.spark = sparkSessionProvider.getConfiguredSparkSession();
    }

    protected void insertTable(TableInfo info, Dataset<Row> dataFrame)
            throws DomainExecutorException {
        logger.info("DomainExecutor:: insertTable");
        String tablePath = storage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        logger.info("Domain insert to disk started");
        try {
            if (!storage.exists(spark, info)) {
                storage.create(tablePath, dataFrame);
                schema.create(info, tablePath, dataFrame);
                logger.info("Creating delta table completed...");
            } else {
                throw new DomainExecutorException("Delta table " + info.getTable() + "already exists");
            }
        } catch(DomainSchemaException dse) {
            throw new DomainExecutorException(dse);
        }
    }

    protected void updateTable(TableInfo info, Dataset<Row> dataFrame)
            throws DomainExecutorException {
        logger.info("DomainExecutor:: updateTable");
        String tablePath = storage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        try {
            if (storage.exists(spark, info)) {
                storage.replace(tablePath, dataFrame);
                schema.replace(info, tablePath, dataFrame);
                logger.info("Updating delta table completed...");
            } else {
                throw new DomainExecutorException("Delta table " + info.getTable() + "doesn't exists");
            }
        } catch(DomainSchemaException dse) {
            throw new DomainExecutorException(dse);
        }
    }

    protected void syncTable(TableInfo info, Dataset<Row> dataFrame)
            throws DomainExecutorException {
        logger.info("DomainExecutor:: syncTable");
        String tablePath = storage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        if (storage.exists(spark, info)) {
            storage.reload(tablePath, dataFrame);
            logger.info("Syncing delta table completed..." + info.getTable());
        } else {
            throw new DomainExecutorException("Delta table " + info.getTable() + "doesn't exist");
        }
    }

    protected void deleteTable(TableInfo info) throws DomainExecutorException {
        logger.info("DomainOperations:: deleteSchemaAndTableData");
        try {
            if (storage.exists(spark, info)) {
                storage.delete(spark, info);
                schema.drop(info);
                storage.endTableUpdates(spark, info);
            } else {
                throw new DomainExecutorException("Table " + info.getTable() + "doesn't exists");
            }
        } catch(DomainSchemaException dse) {
            throw new DomainExecutorException(dse);
        }
    }

    protected void saveTable(TableInfo info, Dataset<Row> dataFrame,
                             String domainOperation)
            throws DomainExecutorException {
        logger.info("DomainOperations::saveTable");
        if (domainOperation.equalsIgnoreCase("insert")) {
            insertTable(info, dataFrame);
        } else if (domainOperation.equalsIgnoreCase("update")) {
            updateTable(info, dataFrame);
        } else if (domainOperation.equalsIgnoreCase("sync")) {
            syncTable(info, dataFrame);
        } else {
            // Handle invalid operation type
            logger.error("Invalid operation type " + domainOperation);
            throw new DomainExecutorException("Invalid operation type " + domainOperation);
        }
        storage.endTableUpdates(spark, info);
    }

    protected void saveViolations(TableInfo target, Dataset<Row> dataFrame) {
        String tablePath = storage.getTablePath(target.getPrefix(), target.getSchema(), target.getTable());
        // save the violations to the specified location
        storage.append(tablePath, dataFrame);
        storage.endTableUpdates(spark, target);
    }

    public Dataset<Row> getAllSourcesForTable(String sourcePath, String source,
                                              TableTuple exclude) throws DomainExecutorException {
        if(exclude == null || !exclude.asString().equalsIgnoreCase(source)) {
            try {
                TableTuple full = new TableTuple(source);
                Dataset<Row> dataFrame = storage.load(
                    spark,
                    new TableInfo(sourcePath, hiveDatabaseName, full.getSchema(), full.getTable())
                );
                if(dataFrame != null) {
                    logger.info("Loaded source '" + full.asString() +"'.");
                    return dataFrame;
                } else {
                    logger.error("Source " + full.asString() + " not found");
                    throw new DomainExecutorException("Source " + full.asString() + " not found");
                }
            } catch(Exception e) {
                logger.error("Unable to get the source information", e);
            }
        } else {
            //TODO: this condition only for unit test
            // we already have this table
            logger.info("Table already present " + exclude.asString());
        }
        return null;
    }


    protected Dataset<Row> applyViolations(Dataset<Row> dataFrame, List<ViolationDefinition> violations) {
        var sourceDataframe = dataFrame;
        for (val violation : violations) {
            val applyCondition = violation.getCheck();
            val violationsDataframe = sourceDataframe.where(applyCondition).toDF();
            if (violationsDataframe.isEmpty()) {
                logger.info("No Violation records found for condition " + applyCondition);
            }
            else {
                logger.info("Removing violation records");
                TableInfo info = new TableInfo(
                    targetRootPath,
                    hiveDatabaseName,
                    violation.getLocation(),
                    violation.getName()
                );
                saveViolations(info, violationsDataframe);
                sourceDataframe = sourceDataframe.except(violationsDataframe);
            }
        }
        return sourceDataframe;
    }

    protected Dataset<Row> applyTransform(Map<String, Dataset<Row>> dfs, TransformDefinition transform)
            throws DomainExecutorException {
        List<String> srcs = new ArrayList<>();
        String view = transform.getViewText().toLowerCase();
        try {
            if (view.isEmpty()) {
                logger.error("View text is empty");
                throw new DomainExecutorException("View text is empty");
            }

            for (String source : transform.getSources()) {
                String src = source.toLowerCase().replace(".", "__");
                Dataset<Row> df_source = dfs.get(source);
                if (df_source != null) {
                    df_source.createOrReplaceTempView(src);
                    logger.info("Added view '" + src + "'");
                    srcs.add(src);
                    if (schemaContains(df_source, "_operation") &&
                            schemaContains(df_source, "_timestamp")) {
                        view = view.replace(" from ", ", " + src + "._operation, " + src + "._timestamp from ");
                    }
                }
                logger.info(view);
                view = view.replace(source, src);
            }
            logger.info("Executing view '" + view + "'...");
            // This will validate whether the given SQL is valid
            return validateSQLAndExecute(view);
        } finally {
            if (!view.isEmpty()) {
                for (String source : srcs) {
                    spark.catalog().dropTempView(source);
                }
            }
        }
    }

    private Dataset<Row> validateSQLAndExecute(String query) {
        Dataset<Row> dataframe = null;
        if (!query.isEmpty()) {
            try {
                spark.sql(query).queryExecution().explainString(ExplainMode.fromString("simple"));
                dataframe = spark.sqlContext().sql(query).toDF();
            } catch (Exception e) {
                logger.error("SQL text is invalid: " + query);
            }
        }
        return dataframe;
    }


    // TODO: Mapping will be enhanced at later stage in future user stories
    protected Dataset<Row> applyMappings(Dataset<Row> dataFrame, TableDefinition.MappingDefinition mapping) {
        if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()
                && !dataFrame.isEmpty()) {
            return dataFrame.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return dataFrame;
    }

    public Dataset<Row> apply(TableDefinition table, Map<String, Dataset<Row>> sourceTableMap)
            throws DomainExecutorException {
        try {

            logger.info("Apply Method for " + table.getName() + "...");
            // Transform
            Map<String,Dataset<Row>> refs = new HashMap<>();
            // Add sourceTable if present
            if (sourceTableMap != null && sourceTableMap.size() > 0) {
                refs.putAll(sourceTableMap);
            } else if(table.getTransform() != null && table.getTransform().getSources() != null
                    && table.getTransform().getSources().size() > 0) {
                for (String source : table.getTransform().getSources()) {
                    Dataset<Row> sourceDataFrame = this.getAllSourcesForTable(sourceRootPath, source, null);
                    if (sourceDataFrame != null) {
                        refs.put(source.toLowerCase(), sourceDataFrame);
                    } else {
                        logger.info("Unable to load source '" + source +"' for Table Definition '" + table.getName() + "'");
                        throw new DomainExecutorException("Unable to load source '" + source +
                                "' for Table Definition '" + table.getName() + "'");
                    }
                }
            } else {
                //TODO: Expecting this condition should never be reached
                logger.info("TableDefinition is invalid");
            }


            logger.info("'" + table.getName() + "' has " + refs.size() + " references to tables...");
            Dataset<Row> transformedDataFrame = applyTransform(refs, table.getTransform());

            logger.info("Apply Violations for " + table.getName() + "...");
            // Process Violations - we now have a subset
            Dataset<Row> postViolationsDataFrame = applyViolations(transformedDataFrame, table.getViolations());

            logger.info("Apply Mappings for " + table.getName() + "...");
            // Mappings
            return applyMappings(postViolationsDataFrame, table.getMapping());

        } catch(Exception e) {
            logger.info("Apply Domain for " + table.getName() + " failed.");
            throw new DomainExecutorException("Apply Domain for " + table.getName() + " failed.");
        }
        finally {
            logger.info("Apply Domain process for " + table.getName() + " completed.");
        }
    }


    public void doFullDomainRefresh(DomainDefinition domainDefinition,
                       String domainName,
                       String domainTableName,
                       String domainOperation) {
        try {
            if (domainOperation.equalsIgnoreCase("insert") ||
                    domainOperation.equalsIgnoreCase("update") ||
                    domainOperation.equalsIgnoreCase("sync")) {

                val table = domainDefinition.getTables().stream()
                        .filter(t -> domainTableName.equals(t.getName()))
                        .findAny()
                        .orElse(null);

                if (table == null) {
                    logger.error("Table " + domainTableName + " not found");
                    throw new DomainExecutorException("Table " + domainTableName + " not found");
                } else {
                    // TODO no source table and df they are required only for unit testing
                    val dfTarget = apply(table, null);
                    saveTable(
                        new TableInfo(
                            targetRootPath,
                            hiveDatabaseName,
                            domainDefinition.getName(),
                            table.getName()
                        ),
                        dfTarget,
                        domainOperation
                    );
                }
            } else if (domainOperation.equalsIgnoreCase("delete")) {
                logger.info("domain operation is delete");
                deleteTable(new TableInfo(
                    targetRootPath,
                    hiveDatabaseName,
                    domainName,
                    domainTableName
                ));
            } else {
                val message = "Unsupported domain operation: '" + domainOperation + "'";
                logger.error(message);
                throw new UnsupportedOperationException(message);
            }
        } catch(Exception | DomainExecutorException e) {
            logger.error("Domain executor failed" + e.getMessage());
        }
    }

    private boolean schemaContains(Dataset<Row> dataFrame, String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }
}
