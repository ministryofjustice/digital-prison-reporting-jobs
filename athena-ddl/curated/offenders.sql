CREATE EXTERNAL TABLE dms_curated.nomis_offenders
LOCATION 's3://dpr-dms-curated-zone-development/nomis/offenders/'
TBLPROPERTIES (
'table_type'='DELTA'
);