package uk.gov.justice.digital.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.domains.model.DomainRepoRecord;
import uk.gov.justice.digital.domains.model.TableDefinition;
import uk.gov.justice.digital.service.DeltaLakeService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.col;

public class DomainRepository {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    // sourceDomains // TODO not required
    protected String domainFilesPath; // TODO not required
    // delta table repo
    protected String domainRepositoryPath;
    protected DeltaLakeService service = new DeltaLakeService();
    protected final static String SCHEMA = "domain-repository";
    protected final static String TABLE = "domain";
    private SparkSession spark;
    private DynamoDBClient dynamoDBClient;

    public DomainRepository(final SparkSession spark,
                            final String domainFilesPath,
                            final String domainRepositoryPath,
                            final DynamoDBClient dynamoDBClient) {
        this.spark = spark;
        this.domainFilesPath = domainFilesPath;
        this.domainRepositoryPath = domainRepositoryPath;
        this.dynamoDBClient = dynamoDBClient;
        System.out.println("Domain Repository(files='" + this.domainFilesPath + "';repo='" + this.domainRepositoryPath + "')");
    }

    public boolean exists() {
        return service.exists(domainRepositoryPath, SCHEMA, TABLE);
    }

    public void touch() {
        load();
    }

    protected void load() {
        // this loads all the domains that are in the bucket into a repository for referencing
        // TODO Extract this String checks into a utility
        if(domainFilesPath == null || domainFilesPath.isEmpty()) {
            throw new IllegalArgumentException("No Domain Files Path. The repository is read-only and cannot load domains.");
        }

        try {
            final Dataset<Row> df_domains = spark.read()
                    .option("wholetext", true)
                    .option("recursiveFileLookup", true)
                    .text(domainFilesPath);

            final List<DomainRepoRecord> records = new ArrayList<>();

            final List<Row> listDomains = df_domains.collectAsList();

            System.out.println("Processing domains (" + listDomains.size() + "...)");
            for(final Row row : listDomains) {
                loadOne("", row.getString(0), records);
            }

            // replace the repository
            List<Row> rows = new ArrayList<>();
            for(final DomainRepoRecord record : records) {
                rows.add(record.toRow());
            }

            final Dataset<Row> df = spark.createDataFrame(rows, DomainRepoRecord.SCHEMA);
            if(service.exists(domainFilesPath, SCHEMA, TABLE)) {
                service.replace(domainRepositoryPath, SCHEMA, TABLE, df);
            } else {
                service.append(domainRepositoryPath, SCHEMA, TABLE, df);
            }
        } catch(Exception e) {
            handleError(e);
        }
    }

    protected void loadOne(final String filename, final String json, List<DomainRepoRecord> records ) {
        try {

            final DomainDefinition domain = MAPPER.readValue(json, DomainDefinition.class);
            System.out.println("Processing domain '" + domain.getName() + "'");

            DomainRepoRecord record = new DomainRepoRecord();

            record.setActive(true);
            record.setName(domain.getName());
            record.setVersion(domain.getVersion());
            record.setLocation(filename);
            record.setDefinition(json);
            for(final TableDefinition table : domain.getTables()) {
                for(final String source : table.getTransform().getSources()) {
                    record.getSources().add(source);
                }
            }

            records.add(record);

        } catch (Exception e) {
            handleError(e);
        }
    }

    public Set<DomainDefinition> getForName(final String domainTableName, final String domainId) {
        Set<DomainDefinition> domains = new HashSet<>();
        DomainDefinition domain = dynamoDBClient.getDomainDefinition(domainTableName, domainId);
        domains.add(domain);
        return domains;
    }

//    public Set<DomainDefinition> getForName(final String name) {
//        Set<DomainDefinition> domains = new HashSet<>();
//
//        try {
//            final Dataset<Row> df = getDomainRepository();
//            if(df != null) {
//                String[] names = name.split("[.]");
//                if(names.length >= 2) {
//                    final List<String> results = df
//                            .where("name ='" + names[0].toLowerCase() + "'")
//                            .select(col("definition")).as(Encoders.STRING())
//                            .collectAsList();
//
//                    for (final String result : results) {
//                        domains.add(MAPPER.readValue(result, DomainDefinition.class));
//                    }
//                    for(DomainDefinition domain: domains){
//                        domain.getTables().removeIf(table -> !table.getName().equalsIgnoreCase(names[1]));
//                    }
//                } else {
//                    final List<String> results = df
//                            .where("name ='" + name.toLowerCase() + "'")
//                            .select(col("definition")).as(Encoders.STRING())
//                            .collectAsList();
//
//                    for (final String result : results) {
//                        domains.add(MAPPER.readValue(result, DomainDefinition.class));
//                    }
//                }
//            } else {
//                throw new RuntimeException("Domain Repository (" + domainRepositoryPath + "/" + SCHEMA + "/" + TABLE + ") does not exist. Please refresh the domain repository");
//            }
//        } catch(Exception e) {
//            handleError(e);
//        }
//        return domains;
//    }

    // TODO this is replaced with DynamoDB
//    protected Dataset<Row> getDomainRepository() {
//        if(service.exists(domainRepositoryPath, SCHEMA, TABLE)) {
//            return service.load(domainRepositoryPath, SCHEMA, TABLE);
//        }
//        return null;
//    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
