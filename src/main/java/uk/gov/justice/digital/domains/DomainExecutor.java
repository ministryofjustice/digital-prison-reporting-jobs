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


    // TODO try to pull doFull(final TableTuple sourceTable) and
    //  getTablesChangedForSourceTable(final TableTuple sourceTable) from POC code
    public void doFull() {

        // Replace all tables
        final List<TableDefinition> tables = domainDefinition.getTables();
        for(final TableDefinition table : tables) {

            try {
                // (3) run transforms
                // (4) run violations
                // (5) run mappings if available
                // TODO no source table and df they are required only for unit testing
                final Dataset<Row> df_target = apply(table, null, null);

                // (6) save materialised view
                final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
                saveFull(targetInfo, df_target);
            } catch(Exception e) {
                handleError(e);
            }
        }
    }

    protected Dataset<Row> apply(final TableDefinition table, final TableTuple sourceTable, final Dataset<Row> df) {
        try {

            System.out.println("DomainExecutor::applyTransform(" + table.getName() + ")...");
            // Transform
            final Map<String, Dataset<Row>> refs = this.getAllSourcesForTable(table, sourceTable);
            // Add sourceTable if present
            if(sourceTable != null && df != null) {
                refs.put(sourceTable.asString().toLowerCase(), df);
            }
            System.out.println("'" + table.getName() + "' has " + refs.size() + " references to tables...");
            final Dataset<Row> df_transform = applyTransform(refs, table.getTransform());

            System.out.println("DomainExecutor::applyViolations(" + table.getName() + ")...");
            // Process Violations - we now have a subset
            final Dataset<Row> df_postViolations = applyViolations(df_transform, table.getViolations());

            System.out.println("DomainExecutor::applyMappings(" + table.getName() + ")...");
            // Mappings
            final Dataset<Row> df_postMappings = applyMappings(df_postViolations, table.getMapping());

            return df_postMappings;

        } catch(Exception e) {
            System.out.println("DomainExecutor::apply(" + table.getName() + ") failed.");
            handleError(e);
            return df;
        }
        finally {
            System.out.println("DomainExecutor::apply(" + table.getName() + ") completed.");
        }
    }

    protected Dataset<Row> applyViolations(final Dataset<Row> df, final List<ViolationDefinition> violations) {
        Dataset<Row> working_df = df;
        for(final ViolationDefinition violation : violations) {
            final Dataset<Row> df_violations = working_df.where("not(" + violation.getCheck() + ")").toDF();
            if(!df_violations.isEmpty()) {
                TableInfo info = TableInfo.create(targetRootPath, violation.getLocation(), violation.getName());
                saveViolations(info, df_violations);
                working_df = working_df.except(df_violations);
            }
        }
        return working_df;
    }

    protected void saveViolations(final TableInfo target, final Dataset<Row> df) {
        // save the violations to the specified location
        deltaService.append(target.getPrefix(), target.getSchema(), target.getTable(), df);
        deltaService.endTableUpdates(target.getPrefix(), target.getSchema(), target.getTable());
    }

    protected Dataset<Row> applyMappings(final Dataset<Row> df, final MappingDefinition mapping) {
        if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()) {
            return df.sqlContext().sql(mapping.getViewText()).toDF();
        }
        return df;
    }

    protected void saveFull(final TableInfo info, final Dataset<Row> df) {
        deltaService.replace(info.getPrefix(), info.getSchema(), info.getTable(), df);
        deltaService.endTableUpdates(info.getPrefix(), info.getSchema(), info.getTable());
    }

    protected Map<String, Dataset<Row>> getAllSourcesForTable(final TableDefinition table, final TableTuple exclude) {
        Map<String,Dataset<Row>> fullSources = new HashMap<String,Dataset<Row>>();
        if(table.getTransform() != null && table.getTransform().getSources() != null && table.getTransform().getSources().size() > 0) {
            for( final String source : table.getTransform().getSources()) {
                if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
                    // we already have this table
                } else {
                    try {
                        TableTuple full = new TableTuple(source);
                        final Dataset<Row> df = deltaService.load(sourceRootPath, full.getSchema(), full.getTable());
                        if(df == null) {
                            System.err.println("Unable to load source '" + source +"' for Table Definition '" + table.getName() + "'");
                        } else {
                            System.out.println("Loaded source '" + full.asString() +"'.");
                            fullSources.put(source.toLowerCase(), df);
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
        final List<String> srcs = new ArrayList<String>();
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
                // continue;
            }
        }
    }

    protected boolean schemaContains(final Dataset<Row> df, final String field) {
        return Arrays.<String>asList(df.schema().fieldNames()).contains(field);
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
