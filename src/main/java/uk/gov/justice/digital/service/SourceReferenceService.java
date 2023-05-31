package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.val;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.io.InputStream;
import java.util.*;

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
        sources.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
        sources.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
        sources.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
//        sources.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));
    }

    public Optional<SourceReference> getSourceReference(String source, String table) {
        val key = generateKey(source, table);

        val sourceReference = schemaClient.getSchema(key).map(this::createFromAvroSchema);

        if (sourceReference.isPresent()) return sourceReference;
        else {
            logger.warn("No SourceReference found in registry for {} - falling back to hardcoded resources", key);
            return Optional.ofNullable(sources.get(generateKey(source, table)));
        }
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

    private SourceReference createFromAvroSchema(String avroSchema) {
        val parsedAvro = parseAvroString(avroSchema);

        return new SourceReference(
                parsedAvro.getKey(),
                parsedAvro.getService(),
                parsedAvro.getName(),
                parsedAvro.findPrimaryKey()
                        .orElseThrow(() -> new IllegalStateException("No primary key found in schema: " + avroSchema)),
                converter.convert(avroSchema)
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

    @Data
    @Builder
    @Jacksonized
    // Private class that declares only the fields we are interested in when constructing a SourceReference.
    private static class AvroSchema {
        private final String service;
        private final String name;
        private final List<Map<String, String>> fields;

        public String getKey() {
            return String.join(".", service, name).toLowerCase();
        }

        public Optional<String> findPrimaryKey() {
            return fields.stream()
                    .filter(f -> f.containsKey("key") && f.get("key").equals("primary"))
                    .map(f -> f.get("name"))
                    .findFirst();
        }
    }

}
