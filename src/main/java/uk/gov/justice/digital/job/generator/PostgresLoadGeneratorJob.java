package uk.gov.justice.digital.job.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.generator.DataGenerationConfig;
import uk.gov.justice.digital.datahub.model.generator.PostgresSecrets;
import uk.gov.justice.digital.job.HiveTableCreationJob;
import uk.gov.justice.digital.job.PicocliMicronautExecutor;

import javax.inject.Inject;
import java.sql.*;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static picocli.CommandLine.Command;

@Command(name = "PostgresLoadGeneratorJob")
public class PostgresLoadGeneratorJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PostgresLoadGeneratorJob.class);

    JobArguments arguments;
    private final SecretsManagerClient secretsManagerClient;

    @Inject
    public PostgresLoadGeneratorJob(
            JobArguments arguments,
            SecretsManagerClient secretsManagerClient
    ) {
        this.arguments = arguments;
        this.secretsManagerClient = secretsManagerClient;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(PostgresLoadGeneratorJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("PostgresLoadGeneratorJob running");
            String secretId = arguments.getSecretId();
            int batchSize = arguments.getTestDataBatchSize();
            int parallelism = arguments.getTestDataParallelism();
            long interBatchDelay = arguments.getTestDataInterBatchDelayMillis();
            long runDuration = arguments.getRunDuration();

            Class.forName("org.postgresql.Driver");
            PostgresSecrets credentials = secretsManagerClient.getSecret(secretId, PostgresSecrets.class);
            DataGenerationConfig runConfig = new DataGenerationConfig(batchSize, parallelism, interBatchDelay, runDuration);
            generateTestData(credentials, runConfig);

            logger.info("PostgresLoadGeneratorJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    public void generateTestData(PostgresSecrets credentials, DataGenerationConfig runConfig) throws SQLException, InterruptedException {
        Connection connection = null;
        PreparedStatement statement = null;
        long startTime = System.currentTimeMillis();

        try {
            logger.debug("Connecting to {}", credentials.getJdbcUrl());
            connection = DriverManager.getConnection(credentials.getJdbcUrl(), credentials.getUsername(), credentials.getPassword());
            connection.setAutoCommit(false);

            while (System.currentTimeMillis() - startTime < runConfig.getRunDuration()) {
                for (int i = 1; i <= runConfig.getParallelism(); i++) {
                    String insertSql = "INSERT INTO test_table(data) VALUES (?) ON CONFLICT (id) DO UPDATE SET data = ?";
                    statement = connection.prepareStatement(insertSql);
                    for (int j = 1; j <= runConfig.getBatchSize(); j++) {
                        String data = UUID.randomUUID().toString();
                        statement.setString(1, data);
                        statement.setString(2, data);

                        statement.addBatch();
                        logger.debug("Added record {} to batch batch: {}", j, i);
                    }

                    int[] results = statement.executeBatch();
                    logger.info("Total records inserted: {}", Arrays.stream(results).sum());
                    connection.commit();
                    TimeUnit.MILLISECONDS.sleep(runConfig.getInterBatchDelay());
                }
            }
        } catch (SQLException | InterruptedException e) {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
            throw e;
        }
    }
}
