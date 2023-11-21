CREATE EXTERNAL TABLE structured.nomis_agency_internal_locations
LOCATION 's3://dpr-structured-zone-test/nomis/agency_internal_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);