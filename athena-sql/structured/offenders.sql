CREATE EXTERNAL TABLE structured.nomis_offenders
LOCATION 's3://dpr-structured-zone-test/nomis/offenders/'
TBLPROPERTIES (
'table_type'='DELTA'
);