package uk.gov.justice.digital.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SourceReferenceServiceTest {

	@Test
	public void shouldConvert_OMS_OWNER_OFFENDERS_to_nomis_offenders() {
		String source = SourceReferenceService.getSource("OMS_OWNER", "OFFENDERS");
		String table = SourceReferenceService.getTable("OMS_OWNER", "offendERS");

		assertEquals("nomis", source);
		assertEquals("offenders", table);
	}

	@Test
	public void shouldConvert_OMS_OWNER_OFFENDERS_to_nomis_offender_bookings() {
		String source = SourceReferenceService.getSource("OMS_OWNER", "OFFENDER_BOOKINGS");
		String table = SourceReferenceService.getTable("OMS_OWNER", "offendER_BOOKINGS");

		assertEquals("nomis", source);
		assertEquals("offender_bookings", table);
	}

	@Test
	public void shouldGetTheRightPrimaryKey() {
		assertEquals("OFFENDER_ID", SourceReferenceService.getPrimaryKey("OMS_OWNER.OFFENDERS"));
		assertEquals("OFFENDER_BOOK_ID", SourceReferenceService.getPrimaryKey("OMS_OWNER.OFFENDER_BOOKINGS"));
	}

}
