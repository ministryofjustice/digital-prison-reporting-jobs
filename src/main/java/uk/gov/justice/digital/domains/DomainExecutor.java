package uk.gov.justice.digital.domains;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition.MappingDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition.TransformDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.domains.model.TableTuple;
import uk.gov.justice.digital.service.DeltaLakeService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class DomainExecutor {

    // core initialised values
    // sourceRootPath
    // targetRootPath
    // domainDefinition
    protected String sourceRootPath;
    protected String targetRootPath;
    protected DomainDefinition domainDefinition;

    protected DeltaLakeService deltaService = new DeltaLakeService();

    public DomainExecutor(final String sourceRootPath, final String targetRootPath, final DomainDefinition domain) {
        this.sourceRootPath = sourceRootPath;
        this.targetRootPath = targetRootPath;
        this.domainDefinition = domain;
    }


    // TODO this is only for unit testing purpose
    public void doFull(final TableTuple sourceTable) {

        final TableInfo sourceInfo = TableInfo.create(sourceRootPath, sourceTable.getSchema(), sourceTable.getTable());
        final Dataset<Row> df_source = deltaService.load(sourceInfo.getPrefix(), sourceInfo.getSchema(), sourceInfo.getTable());

        final List<TableDefinition> tables = getTablesChangedForSourceTable(sourceTable);
        for(final TableDefinition table : tables) {

            try {
                final Dataset<Row> df_target = apply(table, sourceTable, df_source);
                final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
                saveFull(targetInfo, df_target);
            } catch(Exception e) {
                handleError(e);
            }
        }
    }

    protected List<TableDefinition> getTablesChangedForSourceTable(final TableTuple sourceTable) {
        List<TableDefinition> tables = new ArrayList<>();
        for(final TableDefinition table : domainDefinition.getTables()) {
            for( final String source : table.getTransform().getSources()) {
                if(sourceTable != null && sourceTable.asString().equalsIgnoreCase(source)) {
                    tables.add(table);
                    break;
                }
            }
        }
        return tables;
    }

    public void doFull(final String domainOperation) {
        final List<TableDefinition> tables = domainDefinition.getTables();
        for(final TableDefinition table : tables) {

            try {
                // TODO no source table and df they are required only for unit testing
                final Dataset<Row> df_target = apply(table, null, null);
                final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
                // Create an Enum for insert, update, sync and delete
                if(domainOperation.equalsIgnoreCase("insert") ||
                        domainOperation.equalsIgnoreCase("update") ||
                        domainOperation.equalsIgnoreCase("sync")) {
                    System.out.println("domain operation is insert/update/sync");
                    saveFull(targetInfo, df_target);
                }
                else if(domainOperation.equalsIgnoreCase("delete")) {
                    System.out.println("domain operation is delete");
                    deleteFull(targetInfo);
                }
                else throw new UnsupportedOperationException("Unsupported domain operation.");
            } catch(Exception e) {
                handleError(e);
            }
        }
    }

    protected Dataset<Row> apply(final TableDefinition table, final TableTuple sourceTable, final Dataset<Row> dataFrame) {
        try {

            System.out.println("DomainExecutor::applyTransform(" + table.getName() + ")...");
            // Transform
            final Map<String, Dataset<Row>> refs = this.getAllSourcesForTable(table, sourceTable);
            // Add sourceTable if present
            if(sourceTable != null && dataFrame != null) {
                refs.put(sourceTable.asString().toLowerCase(), dataFrame);
            }
            System.out.println("'" + table.getName() + "' has " + refs.size() + " references to tables...");
            final Dataset<Row> transformedDataFrame = applyTransform(refs, table.getTransform());

            System.out.println("DomainExecutor::applyViolations(" + table.getName() + ")...");
            // Process Violations - we now have a subset
            final Dataset<Row> postViolationsDataFrame = applyViolations(transformedDataFrame, table.getViolations());

            System.out.println("DomainExecutor::applyMappings(" + table.getName() + ")...");
            // Mappings
            final Dataset<Row> postMappingsDataFrame = applyMappings(postViolationsDataFrame, table.getMapping());

            return postMappingsDataFrame;

        } catch(Exception e) {
            System.out.println("DomainExecutor::apply(" + table.getName() + ") failed.");
            handleError(e);
            return dataFrame;
        }
        finally {
            System.out.println("DomainExecutor::apply(" + table.getName() + ") completed.");
        }
    }

    protected Dataset<Row> applyViolations(final Dataset<Row> dataFrame, final List<ViolationDefinition> violations) {
        Dataset<Row> violationsDataFrame = dataFrame;
        for(final ViolationDefinition violation : violations) {
            final Dataset<Row> df_violations = violationsDataFrame.where("not(" + violation.getCheck() + ")").toDF();
            if(!df_violations.isEmpty()) {
                TableInfo info = TableInfo.create(targetRootPath, violation.getLocation(), violation.getName());
                saveViolations(info, df_violations);
                violationsDataFrame = violationsDataFrame.except(df_violations);
            }
        }
        return violationsDataFrame;
    }

    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        // save the violations to the specified location
        deltaService.append(target.getPrefix(), target.getSchema(), target.getTable(), dataFrame);
        deltaService.endTableUpdates(target.getPrefix(), target.getSchema(), target.getTable());
    }

    protected Dataset<Row> applyMappings(final Dataset<Row> dataFrame, final MappingDefinition mapping) {
        if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()) {
            return dataFrame.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return dataFrame;
    }

    protected void saveFull(final TableInfo info, final Dataset<Row> dataFrame) {
        System.out.println("DomainExecutor:: saveFull");
        deltaService.replace(info.getPrefix(), info.getSchema(), info.getTable(), dataFrame);
        deltaService.endTableUpdates(info.getPrefix(), info.getSchema(), info.getTable());
        deltaService.vacuum(info.getPrefix(), info.getSchema(), info.getTable());
    }

    protected void deleteFull(final TableInfo info) {
        System.out.println("DomainExecutor:: deleteFull");
        deltaService.delete(info.getPrefix(), info.getSchema(), info.getTable());
        deltaService.vacuum(info.getPrefix(), info.getSchema(), info.getTable());
    }


    protected Map<String, Dataset<Row>> getAllSourcesForTable(final TableDefinition table, final TableTuple exclude) {
        Map<String,Dataset<Row>> fullSources = new HashMap<>();
        if(table.getTransform() != null && table.getTransform().getSources() != null && table.getTransform().getSources().size() > 0) {
            for( final String source : table.getTransform().getSources()) {
                if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
                    // we already have this table
                } else {
                    try {
                        TableTuple full = new TableTuple(source);
                        final Dataset<Row> dataFrame = deltaService.load(sourceRootPath, full.getSchema(), full.getTable());
                        if(dataFrame == null) {
                            System.err.println("Unable to load source '" + source +"' for Table Definition '" + table.getName() + "'");
                        } else {
                            System.out.println("Loaded source '" + full.asString() +"'.");
                            fullSources.put(source.toLowerCase(), dataFrame);
                        }
                    } catch(Exception e) {
                        handleError(e);
                    }
                }
            }
        }
        return fullSources;
    }

    protected Dataset<Row> applyTransform(final Map<String,Dataset<Row>> dfs, final TransformDefinition transform) {
        final List<String> srcs = new ArrayList<>();
        SparkSession spark = null;
        try {
            String view = transform.getViewText().toLowerCase();
            boolean incremental = false;
            for(final String source : transform.getSources()) {
                final String src = source.toLowerCase().replace(".","__");
                final Dataset<Row> df_source = dfs.get(source);
                if(df_source != null) {
                    df_source.createOrReplaceTempView(src);
                    System.out.println("Added view '" + src +"'");
                    srcs.add(src);
                    if(!incremental &&
                            schemaContains(df_source, "_operation") &&
                            schemaContains(df_source, "_timestamp"))
                    {
                        view = view.replace(" from ", ", " + src +"._operation, " + src + "._timestamp from ");
                        incremental = true;
                    }
                    if(spark == null) {
                        spark = df_source.sparkSession();
                    }
                }
                view = view.replace(source, src);
            }
            System.out.println("Executing view '" + view + "'...");
            return spark == null ? null : spark.sqlContext().sql(view).toDF();
        } catch(Exception e) {
            handleError(e);
            return null;
        } finally {
            try {
                if(spark != null) {
                    for(final String source : srcs) {
                        spark.catalog().dropTempView(source);
                    }
                }
            }
            catch(Exception e) {
                // TODO handle errors
                // continue;
            }
        }
    }

    protected boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
