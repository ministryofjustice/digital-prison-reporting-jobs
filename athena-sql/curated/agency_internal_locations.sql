CREATE EXTERNAL TABLE curated.nomis_agency_internal_locations
LOCATION 's3://dpr-curated-zone-test/nomis/agency_internal_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);