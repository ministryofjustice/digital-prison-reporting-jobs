CREATE EXTERNAL TABLE structured.nomis_agency_locations
LOCATION 's3://dpr-structured-zone-test/nomis/agency_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);