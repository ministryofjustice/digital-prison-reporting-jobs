CREATE EXTERNAL TABLE dms_curated.nomis_agency_locations
LOCATION 's3://dpr-dms-curated-zone-development/nomis/agency_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);