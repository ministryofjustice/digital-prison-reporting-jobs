package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import lombok.val;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.client.glue.GlueSchemaClient.GlueSchemaResponse;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Singleton
public class SourceReferenceService {

    private static final Logger logger = LoggerFactory.getLogger(SourceReferenceService.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final GlueSchemaClient schemaClient;
    private final AvroToSparkSchemaConverter converter;

    @Inject
    public SourceReferenceService(GlueSchemaClient schemaClient,
                                  AvroToSparkSchemaConverter converter) {
        this.schemaClient = schemaClient;
        this.converter = converter;
    }

    private static final Map<String, SourceReference> sources = new HashMap<>();

    static {
        sources.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", new SourceReference.PrimaryKey("OFFENDER_ID"), getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
        sources.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", new SourceReference.PrimaryKey("OFFENDER_BOOK_ID"), getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
        sources.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", new SourceReference.PrimaryKey("AGY_LOC_ID"), getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
        sources.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", new SourceReference.PrimaryKey("INTERNAL_LOCATION_ID"), getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));
    }

    public Optional<SourceReference> getSourceReference(String source, String table) {
        val key = generateKey(source, table);

        val sourceReference = schemaClient.getSchema(key).map(this::createFromAvroSchema);

        if (sourceReference.isPresent()) {
            logger.info("Found contract schema for {} in registry", key);
            return sourceReference;
        }
        else {
            logger.warn("No SourceReference found in registry for {} - falling back to hardcoded resources", key);
            return Optional.ofNullable(sources.get(generateKey(source, table)));
        }
    }

    public List<SourceReference> getAllSourceReferences() {
        return schemaClient.getAllSchemas()
                .stream()
                .map(this::createFromAvroSchema)
                .collect(Collectors.toList());
    }

    public SourceReference getSourceReferenceOrThrow(String inputSchemaName, String inputTableName) {
        Optional<SourceReference> maybeSourceRef = getSourceReference(inputSchemaName, inputTableName);
        return maybeSourceRef.orElseThrow(() -> new RuntimeException(format("No schema found for %s/%s", inputSchemaName, inputTableName)));
    }

    private static StructType getSchemaFromResource(String resource) {
        return (StructType) Optional.ofNullable(System.class.getResourceAsStream(resource))
                .map(SourceReferenceService::readInputStream)
                .map(StructType::fromJson)
                .orElseThrow(() -> new IllegalStateException("Failed to read resource: " + resource));
    }

    private static String readInputStream(InputStream is) {
        return new Scanner(is, "UTF-8")
                .useDelimiter("\\A") // Matches the next boundary which in our case will be EOF.
                .next();
    }

    private String generateKey(String source, String table) {
        return String.join(".", source, table).toLowerCase();
    }

    private SourceReference createFromAvroSchema(GlueSchemaResponse response) {
        val parsedAvro = parseAvroString(response.getAvro());

        return new SourceReference(
                response.getId(),
                parsedAvro.getService(),
                parsedAvro.getNameWithoutVersionSuffix(),
                parsedAvro.findPrimaryKey()
                        .orElseThrow(() -> new IllegalStateException("No primary key found in schema: " + response)),
                converter.convert(response.getAvro())
        );

    }

    private AvroSchema parseAvroString(String avro) {
        try {
            return objectMapper.readValue(avro, AvroSchema.class);
        }
        catch (Exception e) {
            throw new RuntimeException("Caught exception when parsing avro schema", e);
        }
    }

    @Builder
    @Jacksonized
    // Private class that declares only the fields we are interested in when constructing a SourceReference.
    private static class AvroSchema {
        @Getter
        private final String service;
        private final String name;
        private final List<Map<String, Object>> fields;

        private boolean isPrimaryKey(Map<String, Object> attributes) {
            return attributes.containsKey("key") &&
                    Optional.ofNullable(attributes.get("key"))
                            .flatMap(this::castToString)
                            .filter(v -> v.equals("primary"))
                            .isPresent();
        }

        private Optional<String> castToString(Object o) {
            return Optional.ofNullable(o)
                    .filter(String.class::isInstance)
                    .map(String.class::cast);
        }

        public Optional<SourceReference.PrimaryKey> findPrimaryKey() {
            return fields.stream()
                    .filter(this::isPrimaryKey)
                    .map(f -> f.get("name"))
                    .collect(Collectors.collectingAndThen(Collectors.toList(), this::createPrimaryKey));
        }

        private Optional<SourceReference.PrimaryKey> createPrimaryKey(List<Object> list) {
            if(list != null && !list.isEmpty()) {
                return Optional.of(new SourceReference.PrimaryKey(list));
            }
            return Optional.empty();
        }

        // If the table name has a version suffix e.g. SOME_TABLE_NAME_16 this must be removed from the value used in
        // the source reference instance. See SourceReferenceServiceTest for more context.
        public String getNameWithoutVersionSuffix() {
            return name.replaceFirst("_\\d+", "")
                    .toLowerCase(Locale.ENGLISH);
        }
    }


}
