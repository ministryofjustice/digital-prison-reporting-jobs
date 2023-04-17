package uk.gov.justice.digital.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
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

public class DomainRepository {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    // sourceDomains // TODO not required
    protected String domainFilesPath; // TODO not required
    protected String domainRepositoryPath;
    protected final static String SCHEMA = "domain-repository";
    protected final static String TABLE = "domain";
    private SparkSession spark;
    private final DynamoDBClient dynamoDBClient;

    protected DeltaLakeService service = new DeltaLakeService();

    public DomainRepository(final SparkSession spark,
                            final DynamoDBClient dynamoDBClient) {
        this.spark = spark;
        this.dynamoDBClient = dynamoDBClient;
    }


    public Set<DomainDefinition> getForName(final String domainTableName, final String domainId) {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        DomainDefinition domain = dynamoDBClient.getDomainDefinition(domainTableName, domainId);
        domains.add(domain);
        return domains;
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
