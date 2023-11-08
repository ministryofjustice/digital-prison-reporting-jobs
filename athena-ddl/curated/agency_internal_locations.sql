CREATE EXTERNAL TABLE dms_curated.nomis_agency_internal_locations
LOCATION 's3://dpr-dms-curated-zone-development/nomis/agency_internal_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);