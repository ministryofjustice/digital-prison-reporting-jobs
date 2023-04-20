package uk.gov.justice.digital.domains;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.domains.model.*;
import uk.gov.justice.digital.domains.model.TableDefinition.TransformDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.digital.domains.service.DomainService;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Logger;

import uk.gov.justice.digital.exceptions.DomainExecutorException;

public class DomainExecutor extends DomainService {

    // core initialised values
    // sourceRootPath
    // targetRootPath
    // domainDefinition
    protected String sourceRootPath;
    protected String targetRootPath;
    protected DomainDefinition domainDefinition;

    static Logger logger = Logger.getLogger(DomainExecutor.class.getName());


    public DomainExecutor(final String sourceRootPath, final String targetRootPath, final DomainDefinition domain) {
        this.sourceRootPath = sourceRootPath;
        this.targetRootPath = targetRootPath;
        this.domainDefinition = domain;
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
//                logger.info("View text is empty");
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
        } catch (Exception e) {
            handleError(e);
            return null;
        } finally {
            try {
                if (spark != null) {
                    for (final String source : srcs) {
                        spark.catalog().dropTempView(source);
                    }
                }
            } catch (Exception e) {
                // TODO handle errors
                // continue;
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

            logger.info("DomainExecutor::applyTransform(" + table.getName() + ")...");
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

            logger.info("DomainExecutor::applyViolations(" + table.getName() + ")...");
            // Process Violations - we now have a subset
            final Dataset<Row> postViolationsDataFrame = applyViolations(transformedDataFrame, table.getViolations());

            logger.info("DomainExecutor::applyMappings(" + table.getName() + ")...");
            // Mappings
            return applyMappings(postViolationsDataFrame, table.getMapping());

        } catch(Exception e) {
            logger.info("DomainExecutor::apply(" + table.getName() + ") failed.");
            handleError(e);
            throw new DomainExecutorException("DomainExecutor::apply(" + table.getName() + ") failed.");
        }
        finally {
            logger.info("Process DomainExecutor::apply(" + table.getName() + ") completed.");
        }
    }

    public void doFull(final String domainOperation) {
        final List<TableDefinition> tables = domainDefinition.getTables();
        for(final TableDefinition table : tables) {
            try {
                // TODO no source table and df they are required only for unit testing
                final Dataset<Row> df_target = apply(table, null);
                final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
                // Create an Enum for insert, update, sync and delete
                if(domainOperation.equalsIgnoreCase("insert") ||
                        domainOperation.equalsIgnoreCase("update") ||
                        domainOperation.equalsIgnoreCase("sync")) {
                    logger.info("domain operation is insert/update/sync");
                    saveFull(targetInfo, df_target);
                }
                else if(domainOperation.equalsIgnoreCase("delete")) {
                    logger.info("domain operation is delete");
                    deleteFull(targetInfo);
                }
                else throw new UnsupportedOperationException("Unsupported domain operation.");
            } catch(Exception e) {
                handleError(e);
            } catch (DomainExecutorException e) {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
            }
        }
    }

    protected boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }

}
