package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.ResourceLoader;
import uk.gov.justice.digital.domains.DomainExecutor;
import uk.gov.justice.digital.domains.DomainExecutorTest;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.domains.service.DomainService;
import uk.gov.justice.digital.exceptions.DomainExecutorException;
import uk.gov.justice.digital.repository.DomainRepository;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class DomainRefreshServiceTest extends DomainService {

    @TempDir
    Path folder;

    protected DomainDefinition getDomain(final String resource) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
        return mapper.readValue(json, DomainDefinition.class);
    }

    protected static SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder()
                .config(sparkConf)
                .master("local[*]")
                .getOrCreate();
    }

    @Test
    public void getString_MatchesValuePassedToDomainRefreshService() {
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        final String sourcePath = folder.toFile().getAbsolutePath()  + "domain/source";
        final String targetPath = folder.toFile().getAbsolutePath()  + "domain/target";
        final DynamoDBClient dynamoDBClient = null;
        String expectedResult = folder.toFile().getAbsolutePath()  + "domain/source";

        DomainRefreshService service = new DomainRefreshService(spark,
                sourcePath, targetPath, null);
        String result = service.sourcePath;
        assertEquals(expectedResult, result);
    }


    @Test
    public void test_processDomain() throws IOException {
        final String domainTableName = "source.table";
        final String domainId = "0000";
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        final DynamoDBClient dynamoDBClient = null;
        final String domainOperation = "insert";
        final String sourcePath = folder.toFile().getAbsolutePath()  + "domain/source";
        final String targetPath = folder.toFile().getAbsolutePath()  + "domain/target";
        DomainRepository repo = new DomainRepository(spark, null);
        final DomainDefinition domain = getDomain("/sample/domain/incident_domain.json");

        try {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') started");
            final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
            executor.doFull(domainOperation);
            System.out.println("DomainRefresh::process('" + domain.getName() + "') completed");
        } catch (Exception e) {
            System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
            handleError(e);
        }
    }

    @Test
    public void test_handle_error() {
        try {
            throw new DomainExecutorException("test message");
        } catch (DomainExecutorException e){
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.err.print(sw.getBuffer().toString());
        } finally {

        }

    }
}
