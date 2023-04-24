package uk.gov.justice.digital.domain;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.*;
import uk.gov.justice.digital.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainOperations;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import uk.gov.justice.digital.exception.DomainExecutorException;

public class DomainExecutor extends DomainOperations {

    // core initialised values
    // sourceRootPath
    // targetRootPath
    // domainDefinition
    protected String sourceRootPath;
    protected String targetRootPath;
    protected DomainDefinition domainDefinition;
    protected DataStorageService storage;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainExecutor.class);


    public DomainExecutor(final String sourceRootPath, final String targetRootPath, final DomainDefinition domain,
                          final DataStorageService storage) {
        this.sourceRootPath = sourceRootPath;
        this.targetRootPath = targetRootPath;
        this.domainDefinition = domain;
        this.storage = storage;
    }

    @Override
    public void saveFull(final TableInfo info, final Dataset<Row> dataFrame, final String domainOperation)
            throws DomainExecutorException {
        logger.info("DomainOperations:: saveFull");
        String tablePath = storage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        if (domainOperation.equalsIgnoreCase("insert")) {
            storage.create(tablePath, dataFrame);
        } else if (domainOperation.equalsIgnoreCase("update")) {
            if (storage.exists(info)) {
                storage.replace(tablePath, dataFrame);
            } else {
                throw new DomainExecutorException("Delta table " + info.getTable() + "doesn't exists");
            }
        } else if (domainOperation.equalsIgnoreCase("sync")) {
            if (storage.exists(info)) {
                storage.reload(tablePath, dataFrame);
            } else {
                throw new DomainExecutorException("Delta table " + info.getTable() + "doesn't exists");
            }
        } else {
            // Handle invalid operation type
            logger.error("Invalid operation type " + domainOperation);
            throw new DomainExecutorException("Invalid operation type " + domainOperation);
        }
        storage.endTableUpdates(info);
        storage.vacuum(info);
    }

    @Override
    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        String tablePath = storage.getTablePath(target.getPrefix(), target.getSchema(), target.getTable());
        // save the violations to the specified location
        storage.append(tablePath, dataFrame);
        storage.endTableUpdates(target);
    }

    public void deleteFull(final TableInfo info) throws DomainExecutorException {
        logger.info("DomainOperations:: deleteFull");
        if (storage.exists(info)) {
            storage.delete(info);
            storage.endTableUpdates(info);
            storage.vacuum(info);
        } else {
            throw new DomainExecutorException("Table " + info.getTable() + "doesn't exists");
        }
    }

    public Dataset<Row> getAllSourcesForTable(final String sourcePath, final String source,
                                              final TableTuple exclude) throws DomainExecutorException {
        if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
            //TODO: this condition only for unit test
            // we already have this table
            logger.info("table already present " + exclude.asString());
        } else {
            try {
                TableTuple full = new TableTuple(source);
                final Dataset<Row> dataFrame = storage.load(
                        TableInfo.create(sourcePath, full.getSchema(), full.getTable()));
                if(dataFrame != null) {
                    logger.info("Loaded source '" + full.asString() +"'.");
                    return dataFrame;
                } else {
                    logger.error("Source " + full.asString() + " not found");
                    throw new DomainExecutorException("Source " + full.asString() + " not found");
                }
            } catch(Exception e) {
                handleError(e);
            }
        }
        return null;
    }



    protected Dataset<Row> applyViolations(final Dataset<Row> dataFrame, final List<ViolationDefinition> violations) {
        Dataset<Row> violationsDataFrame = dataFrame;
        for (final ViolationDefinition violation : violations) {
            final Dataset<Row> df_violations = violationsDataFrame.where("not(" + violation.getCheck() + ")").toDF();
            if (!df_violations.isEmpty()) {
                TableInfo info = TableInfo.create(targetRootPath, violation.getLocation(), violation.getName());
                saveViolations(info, df_violations);
                violationsDataFrame = violationsDataFrame.except(df_violations);
            }
        }
        return violationsDataFrame;
    }

    protected Dataset<Row> applyTransform(final Map<String, Dataset<Row>> dfs, final TransformDefinition transform)
            throws DomainExecutorException {
        final List<String> srcs = new ArrayList<>();
        SparkSession spark = null;
        try {
            String view = transform.getViewText().toLowerCase();
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
                    if (spark == null) {
                        spark = df_source.sparkSession();
                    }
                }
                logger.info(view);
                view = view.replace(source, src);
            }
            logger.info("Executing view '" + view + "'...");
            return spark == null ? null : spark.sqlContext().sql(view).toDF();
        } finally {
            if (spark != null) {
                for (final String source : srcs) {
                    spark.catalog().dropTempView(source);
                }
            }
        }
    }


    // TODO: Mapping will be enhanced at later stage in future user stories
    protected Dataset<Row> applyMappings(final Dataset<Row> dataFrame, final TableDefinition.MappingDefinition mapping) {
        if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()
                && !dataFrame.isEmpty()) {
            return dataFrame.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return dataFrame;
    }

    public Dataset<Row> apply(final TableDefinition table, final Map<String, Dataset<Row>> sourceTableMap)
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
            handleError(e);
            throw new DomainExecutorException("Apply Domain for " + table.getName() + " failed.");
        }
        finally {
            logger.info("Apply Domain process for " + table.getName() + " completed.");
        }
    }

    public void doFull(final String domainName, final String domainTableName, final String domainOperation) {
        try {
            if (domainOperation.equalsIgnoreCase("insert") ||
                    domainOperation.equalsIgnoreCase("update") ||
                    domainOperation.equalsIgnoreCase("sync")) {
                final List<TableDefinition> tables = domainDefinition.getTables();
                TableDefinition table = tables.stream()
                        .filter(t -> domainTableName.equals(t.getName()))
                        .findAny()
                        .orElse(null);
                if (table == null) {
                    logger.error("Table " + domainTableName + " not found");
                    throw new DomainExecutorException("Table " + domainTableName + " not found");
                } else {
                    // TODO no source table and df they are required only for unit testing
                    final Dataset<Row> df_target = apply(table, null);
                    saveFull(TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName()),
                            df_target, domainOperation);
                }
            } else if (domainOperation.equalsIgnoreCase("delete")) {
                logger.info("domain operation is delete");
                deleteFull(TableInfo.create(targetRootPath, domainName, domainTableName));
            } else {
                logger.error("Unsupported domain operation");
                throw new UnsupportedOperationException("Unsupported domain operation.");
            }
        } catch(Exception e) {
            handleError(e);
        } catch (DomainExecutorException e) {
            logger.error("Domain executor failed: ", e);
        }
    }


    protected boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.getBuffer().toString());
    }

}
