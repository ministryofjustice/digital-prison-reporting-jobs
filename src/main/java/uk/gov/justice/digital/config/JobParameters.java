package uk.gov.justice.digital.config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.JobClient;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Singleton
public class JobParameters {

    private static final Logger logger = LoggerFactory.getLogger(JobParameters.class);

    private Map<String, String> config = new HashMap<String, String>();

    @Inject
    public JobParameters(JobClient jobClient) {
        this(jobClient.getJobParameters());
    }

    public JobParameters(Map<String, String> config) {
        this.config.putAll(config.entrySet()
                .stream()
                .map(this::cleanEntryKey)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        logger.info("Job initialised with parameters: {}", config);
    }

    public void parse(String[] args) {
        Map<String, String> params = Arrays.stream(args).collect(new Collector<String, Map<String, String>, Map<String, String>>() {

            private String key;

            @Override
            public Supplier<Map<String, String>> supplier() {
                return () -> new HashMap<>();
            }

            @Override
            public BiConsumer<Map<String, String>, String> accumulator() {
                return (map, value) -> {
                    if (key != null) {
                        map.put(key, value);
                        key = null;
                    } else {
                        key = value.replaceFirst("--", "");
                    }
                };
            }

            @Override
            public BinaryOperator<Map<String, String>> combiner() {
                return (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                };
            }

            @Override
            public Function<Map<String, String>, Map<String, String>> finisher() {
                return Function.identity();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return new HashSet<Characteristics>(Arrays.asList(Characteristics.IDENTITY_FINISH));
            }
        });
        this.config.putAll(params);
    }


    public String getAwsRegion() {
        return getMandatoryProperty("dpr.aws.region");
    }

    public String getAwsKinesisEndpointUrl() {
        return getMandatoryProperty("dpr.aws.kinesis.endpointUrl");
    }

    public String getAwsDynamoDBEndpointUrl() {
        return getMandatoryProperty("dpr.aws.dynamodb.endpointUrl");
    }

    public String getKinesisReaderStreamName() {
        return getMandatoryProperty("dpr.kinesis.reader.streamName");
    }

    public Duration getKinesisReaderBatchDuration() {
        String durationSeconds = getMandatoryProperty("dpr.kinesis.reader.batchDurationSeconds");
        long parsedDuration = Long.parseLong(durationSeconds);
        return Durations.seconds(parsedDuration);
    }

    public Optional<String> getRawS3Path() {
        return getOptionalProperty("dpr.raw.s3.path");
    }

    public Optional<String> getStructuredS3Path() {
        return getOptionalProperty("dpr.structured.s3.path");
    }

    public Optional<String> getViolationsS3Path() {
        return getOptionalProperty("dpr.violations.s3.path");
    }

    public String getCuratedS3Path() {
        return getMandatoryProperty("dpr.curated.s3.path");
    }

    public String getDomainTargetPath() {
        return getMandatoryProperty("dpr.domain.target.path");
    }

    public String getDomainName() {
        return getMandatoryProperty("dpr.domain.name");
    }

    public String getDomainTableName() {
        return getMandatoryProperty("dpr.domain.table.name");
    }

    public String getDomainRegistry() {
        return getMandatoryProperty("dpr.domain.registry");
    }

    public String getDomainOperation() {
        return getMandatoryProperty("dpr.domain.operation");
    }

    public Optional<String> getCatalogDatabase() {
        return getOptionalProperty("dpr.domain.catalog.db");
    }

    private String getMandatoryProperty(String jobParameter) {
        return Optional
                .ofNullable(config.get(jobParameter))
                .orElseThrow(() -> new IllegalStateException("Job Parameter: " + jobParameter + " is not set"));
    }

    // TODO - consider supporting a default value where if no value is provided we throw an exception if there is no
    //        value at all
    private Optional<String> getOptionalProperty(String jobParameter) {
        return Optional
                .ofNullable(config.get(jobParameter));
    }

    // We expect job parameters to be specified with a leading -- prefix e.g. --some.job.setting consistent with how
    // AWS glue specifies job parameters. The prefix is removed to clean up code handling parameters by name.
    private Map.Entry<String, String> cleanEntryKey(Map.Entry<String, String> entry) {
        val cleanedKey = entry.getKey().replaceFirst("--", "");
        return new AbstractMap.SimpleEntry<>(cleanedKey, entry.getValue());
    }

}
