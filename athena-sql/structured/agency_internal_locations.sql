CREATE EXTERNAL TABLE dms_structured.nomis_agency_internal_locations
LOCATION 's3://dpr-dms-structured-zone-development/nomis/agency_internal_locations/'
TBLPROPERTIES (
'table_type'='DELTA'
);