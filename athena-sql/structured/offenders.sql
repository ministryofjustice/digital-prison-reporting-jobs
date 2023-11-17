CREATE EXTERNAL TABLE dms_structured.nomis_offenders
LOCATION 's3://dpr-dms-structured-zone-development/nomis/offenders/'
TBLPROPERTIES (
'table_type'='DELTA'
);