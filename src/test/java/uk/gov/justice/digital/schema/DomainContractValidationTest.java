package uk.gov.justice.digital.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import lombok.val;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

public class DomainContractValidationTest {

    private static final String RESOURCE_PATH = "/contracts";
    private static final String REFERENCE_CONTRACT = "reference-contract.avsc";
    private static final String AGENCY_INTERNAL_LOCATIONS_CONTRACTS = "agency-internal-locations-contract.avsc";
    private static final String OFFENDERS_CONTRACTS = "offenders.avsc";
    private static final String DOMAIN_CONTRACT_SCHEMA = "domain-contract-schema.json";

    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final Schema.Parser avroSchemaParser = new Schema.Parser();

    private static final JsonSchema validator = JsonSchemaFactory
            .getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(getResource(RESOURCE_PATH + "/" + DOMAIN_CONTRACT_SCHEMA));

    @Test
    public void referenceContractShouldBeValidAvro() {
        assertDoesNotThrow(() -> avroSchemaParser.parse(getResource(RESOURCE_PATH + "/" + REFERENCE_CONTRACT)));
    }

    @Test
    public void referenceContractShouldValidateAgainstContractSchema() throws JsonProcessingException {
        val contract = jsonParser.readTree(getResource(RESOURCE_PATH + "/" + REFERENCE_CONTRACT));
        assertEquals(Collections.emptySet(), validator.validate(contract));
    }

    @Test
    public void setAgencyInternalLocationsContractShouldBeValidAvro() {
        assertDoesNotThrow(() ->
                avroSchemaParser.parse(getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_CONTRACTS))
        );
    }

    @Test
    public void agencyInternalLocationsContractShouldValidateAgainstContractSchema() throws JsonProcessingException {
        val contract = jsonParser.readTree(getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_CONTRACTS));
        assertEquals(Collections.emptySet(), validator.validate(contract));
    }

    @Test
    public void setOffendersContractShouldBeValidAvro() {
        assertDoesNotThrow(() ->
                avroSchemaParser.parse(getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACTS))
        );
    }

    @Test
    public void offendersContractShouldValidateAgainstContractSchema() throws JsonProcessingException {
        val contract = jsonParser.readTree(getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACTS));
        assertEquals(Collections.emptySet(), validator.validate(contract));
    }

}
