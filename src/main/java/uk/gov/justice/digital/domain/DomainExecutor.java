package uk.gov.justice.digital.domain;


import lombok.val;
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

    private final DomainSchemaService schema;

    private final SparkSession spark;
    private final String hiveDatabaseName;

    @Inject
    public DomainExecutor(JobParameters jobParameters,
                          DataStorageService storage,
                          DomainSchemaService schema,
                          SparkSessionProvider sparkSessionProvider
                          ) {

        this(   jobParameters.getCuratedS3Path(),
                jobParameters.getDomainTargetPath(),
                storage,
                schema,
                jobParameters.getCatalogDatabase()
                        .orElseThrow(() -> new IllegalStateException(
                                "Hive Catalog database not set - unable to create Hive Catalog schema"
                        )),
                sparkSessionProvider
        );
    }

    public DomainExecutor(String sourceRootPath,
                          String targetRootPath,
                          DataStorageService storage,
                          DomainSchemaService schema,
                          String hiveDatabaseName,
                          SparkSessionProvider sparkSessionProvider) {
        this.sourceRootPath = sourceRootPath;
        this.targetRootPath = targetRootPath;
        this.storage = storage;
        this.schema = schema;
        this.hiveDatabaseName = hiveDatabaseName;
        this.spark = sparkSessionProvider.getConfiguredSparkSession();
    }

    protected void insertTable(final TableInfo info, final Dataset<Row> dataFrame)
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

    protected void updateTable(final TableInfo info, final Dataset<Row> dataFrame)
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

    protected void syncTable(final TableInfo info, final Dataset<Row> dataFrame)
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

    protected void deleteTable(final TableInfo info) throws DomainExecutorException {
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

    protected void saveTable(final TableInfo info, final Dataset<Row> dataFrame,
                          final String domainOperation)
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

    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        String tablePath = storage.getTablePath(target.getPrefix(), target.getSchema(), target.getTable());
        // save the violations to the specified location
        storage.append(tablePath, dataFrame);
        storage.endTableUpdates(spark, target);
    }



    protected Dataset<Row> getAllSourcesForTable(final String sourcePath, final String source,
                                              final TableTuple exclude) throws DomainExecutorException {
        if(exclude == null || !exclude.asString().equalsIgnoreCase(source)) {
            try {
                TableTuple full = new TableTuple(source);
                final Dataset<Row> dataFrame = storage.load(spark,
                        TableInfo.create(sourcePath, hiveDatabaseName, full.getSchema(), full.getTable()));
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


    protected Dataset<Row> applyViolations(final Dataset<Row> dataFrame, final List<ViolationDefinition> violations) {
        Dataset<Row> sourceDataframe = dataFrame;
        for (final ViolationDefinition violation : violations) {
            final String applyCondition = violation.getCheck();
            final Dataset<Row> violationsDataframe = sourceDataframe.where(applyCondition).toDF();
            if (!violationsDataframe.isEmpty()) {
                logger.info("Removing violation records");
                TableInfo info = TableInfo.create(targetRootPath, hiveDatabaseName,
                        violation.getLocation(), violation.getName());
                saveViolations(info, violationsDataframe);
                sourceDataframe = sourceDataframe.except(violationsDataframe);
            } else {
                logger.info("No Violation records found for condition " + applyCondition);
            }
        }
        return sourceDataframe;
    }

    protected Dataset<Row> applyTransform(final Map<String, Dataset<Row>> dfs, final TransformDefinition transform)
            throws DomainExecutorException {
        final List<String> srcs = new ArrayList<>();
        String view = transform.getViewText().toLowerCase();
        try {
            if (view.isEmpty()) {
                logger.error("View text is empty");
                throw new DomainExecutorException("View text is empty");
            }

            for (final String source : transform.getSources()) {
                final String src = source.toLowerCase().replace(".", "__");
                final Dataset<Row> df_source = dfs.get(source);
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
                for (final String source : srcs) {
                    spark.catalog().dropTempView(source);
                }
            }
        }
    }

    private Dataset<Row> validateSQLAndExecute(final String query) {
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
    protected Dataset<Row> applyMappings(final Dataset<Row> dataFrame, final TableDefinition.MappingDefinition mapping) {
        if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()
                && !dataFrame.isEmpty()) {
            return dataFrame.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return dataFrame;
    }

    protected Dataset<Row> apply(final TableDefinition table, final Map<String, Dataset<Row>> sourceTableMap)
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
                for (final String source : table.getTransform().getSources()) {
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
            final Dataset<Row> transformedDataFrame = applyTransform(refs, table.getTransform());

            logger.info("Apply Violations for " + table.getName() + "...");
            // Process Violations - we now have a subset
            final Dataset<Row> postViolationsDataFrame = applyViolations(transformedDataFrame, table.getViolations());

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
                        TableInfo.create(targetRootPath, hiveDatabaseName,
                                domainDefinition.getName(), table.getName()), dfTarget, domainOperation);
                }
            } else if (domainOperation.equalsIgnoreCase("delete")) {
                logger.info("domain operation is delete");
                deleteTable(TableInfo.create(targetRootPath, hiveDatabaseName,
                        domainName, domainTableName));
            } else {
                val message = "Unsupported domain operation: '" + domainOperation + "'";
                logger.error(message);
                throw new UnsupportedOperationException(message);
            }
        } catch(Exception | DomainExecutorException e) {
            logger.error("Domain executor failed" + e.getMessage());
        }
    }

    private boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }
}
