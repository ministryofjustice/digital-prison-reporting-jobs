CREATE EXTERNAL TABLE curated.nomis_agency_locations
LOCATION 's3://dpr-curated-zone-test/nomis/agency_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);