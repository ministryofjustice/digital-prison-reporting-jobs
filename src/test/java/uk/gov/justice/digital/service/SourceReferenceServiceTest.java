package uk.gov.justice.digital.service;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
public class SourceReferenceServiceTest {
	@Test
	public void shouldConvert_SYSTEM_OFFENDERS_to_nomis_offenders() {
		String source = SourceReferenceService.getSource("system.OFFENDERS");
		String table = SourceReferenceService.getTable("system.OFFENDERS");
		
		assertEquals("nomis", source);
		assertEquals("offenders", table);
	
	}
	@Test
	public void shouldConvert_OMS_OWNER_OFFENDERS_to_nomis_offenders() {
		String source = SourceReferenceService.getSource("OMS_OWNER.OFFENDERS");
		String table = SourceReferenceService.getTable("OMS_OWNER.offendERS");
		
		assertEquals("nomis", source);
		assertEquals("offenders", table);
	
	}
	@Test
	public void shouldConvert_SYSTEM_OFFENDERS_to_nomis_offender_bookings() {
		String source = SourceReferenceService.getSource("system.OFFENDER_BOOKINGS");
		String table = SourceReferenceService.getTable("system.OFFENDER_BOOKINGS");
		
		assertEquals("nomis", source);
		assertEquals("offender_bookings", table);
	
	}
	@Test
	public void shouldConvert_OMS_OWNER_OFFENDERS_to_nomis_offender_bookings() {
		String source = SourceReferenceService.getSource("OMS_OWNER.OFFENDER_BOOKINGS");
		String table = SourceReferenceService.getTable("OMS_OWNER.offendER_BOOKINGS");
		
		assertEquals("nomis", source);
		assertEquals("offender_bookings", table);
	}
	@Test
	public void shouldConvert_public_report_to_use_of_force_report() {
		String source = SourceReferenceService.getSource("public.report");
		String table = SourceReferenceService.getTable("public.report");
		
		assertEquals("use_of_force", source);
		assertEquals("report", table);
	}
	@Test
	public void shouldConvert_public_report_to_use_of_force_statement() {
		String source = SourceReferenceService.getSource("public.statement");
		String table = SourceReferenceService.getTable("public.statement");
		
		assertEquals("use_of_force", source);
		assertEquals("statement", table);
	}
	@Test
	public void shouldGetTheRightPrimaryKey() {
		assertEquals("OFFENDER_ID", SourceReferenceService.getPrimaryKey("OMS_OWNER.OFFENDERS"));
		assertEquals("OFFENDER_BOOK_ID", SourceReferenceService.getPrimaryKey("OMS_OWNER.OFFENDER_BOOKINGS"));
		assertEquals("OFFENDER_ID", SourceReferenceService.getPrimaryKey("SYSTEM.OFFENDERS"));
		assertEquals("OFFENDER_BOOK_ID", SourceReferenceService.getPrimaryKey("SYSTEM.OFFENDER_BOOKINGS"));
		assertEquals("id", SourceReferenceService.getPrimaryKey("public.report"));
		assertEquals("id", SourceReferenceService.getPrimaryKey("public.statement"));
	}

}
