CREATE EXTERNAL TABLE dms_structured.nomis_agency_locations
LOCATION 's3://dpr-dms-structured-zone-development/nomis/agency_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);