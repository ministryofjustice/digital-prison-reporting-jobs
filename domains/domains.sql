-- =================================================================
-- establishment/establishment
-- =================================================================
DROP VIEW IF EXISTS domain.establishment_establishment;

CREATE OR REPLACE VIEW domain.establishment_establishment AS 
SELECT 
	prisons.nomis_agency_locations.agy_loc_id as id, 
	prisons.nomis_agency_locations.description as name 
FROM
	prisons.nomis_agency_locations
WITH NO SCHEMA BINDING;



-- =================================================================
-- establishment/living_unit
-- =================================================================
DROP VIEW IF EXISTS domain.establishment_living_unit;

CREATE OR REPLACE VIEW domain.establishment_living_unit AS 
SELECT 
	prisons.nomis_agency_internal_locations.internal_location_id as id, 
	prisons.nomis_agency_internal_locations.internal_location_code as code, 
	prisons.nomis_agency_internal_locations.agy_loc_id as establishment_id, 
	prisons.nomis_agency_internal_locations.description as name 
FROM
	prisons.nomis_agency_internal_locations
WITH NO SCHEMA BINDING;



-- =================================================================
-- prisoner/prisoner
-- =================================================================
DROP VIEW IF EXISTS domain.prisoner_prisoner;

CREATE OR REPLACE VIEW domain.prisoner_prisoner AS 
SELECT o.offender_id_display AS number,
ob.offender_book_id AS id,
ob.living_unit_id AS living_unit_reference,
o.first_name AS firstname,
o.middle_name AS middlename,
o.middle_name_2 AS middename2,
o.last_name AS lastname,
CONCAT(CONCAT(o.last_name, ', '), substring(o.first_name, 1, 1)) AS name,
o.birth_date AS dob,
o.sex_code AS sexcode,
CASE WHEN o.race_code='W8' then 'W3'
WHEN o.race_code='O1' then 'A4'
ELSE o.race_code END AS racecode
FROM prisons.nomis_offender_bookings ob
JOIN prisons.nomis_offenders o ON ob.offender_id=o.offender_id
WITH NO SCHEMA BINDING;



-- =================================================================
-- movement/movement
-- Update
-- =================================================================
DROP VIEW IF EXISTS domain.movement_movement;

CREATE OR REPLACE VIEW domain.movement_movement AS 
SELECT concat(cast(prisons.nomis_offender_external_movements.offender_book_id as varchar), concat('.', cast(prisons.nomis_offender_external_movements.movement_seq as varchar))) as id,
 prisons.nomis_offender_external_movements.offender_book_id as prisoner,
 prisons.nomis_offender_external_movements.movement_date as date,
 prisons.nomis_offender_external_movements.movement_time as time,
 prisons.nomis_offender_external_movements.direction_code as direction,
 prisons.nomis_offender_external_movements.movement_type as type,
 prisons.nomis_offender_external_movements.from_agy_loc_id as origin_code,
 prisons.nomis_offender_external_movements.to_agy_loc_id as destination_code,
 prisons.nomis_offender_external_movements.comment_text as comment,
 prisons.nomis_offender_external_movements.escort_code as escort,
 origin_location.description as origin,
 destination_location.description as destination,
 prisons.nomis_movement_reasons.description as reason,
prisons.nomis_movement_reasons.movement_reason_code as reason_code
from prisons.nomis_offender_external_movements
 join prisons.nomis_movement_reasons
 on prisons.nomis_movement_reasons.movement_type =
 prisons.nomis_offender_external_movements.movement_type
 and prisons.nomis_movement_reasons.movement_reason_code =
 prisons.nomis_offender_external_movements.movement_reason_code
 left join prisons.nomis_agency_locations as origin_location
 on prisons.nomis_offender_external_movements.from_agy_loc_id =
 origin_location.agy_loc_id
 left join prisons.nomis_agency_locations as destination_location
 on prisons.nomis_offender_external_movements.to_agy_loc_id =
 destination_location.agy_loc_id
WITH NO SCHEMA BINDING;


-- =================================================================
-- test/test
-- =================================================================
DROP VIEW IF EXISTS domain.test_test;

