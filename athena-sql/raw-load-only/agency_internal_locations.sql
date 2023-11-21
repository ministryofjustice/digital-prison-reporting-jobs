CREATE EXTERNAL TABLE `raw`.`nomis_agency_internal_locations_load_only`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `INTERNAL_LOCATION_ID`          decimal(10, 0) COMMENT '',
    `INTERNAL_LOCATION_CODE`        string COMMENT '',
    `AGY_LOC_ID`                    string COMMENT '',
    `INTERNAL_LOCATION_TYPE`        string COMMENT '',
    `DESCRIPTION`                   string COMMENT '',
    `SECURITY_LEVEL_CODE`           string COMMENT '',
    `CAPACITY`                      integer COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `PARENT_INTERNAL_LOCATION_ID`   decimal(10, 0) COMMENT '',
    `ACTIVE_FLAG`                   string COMMENT '',
    `LIST_SEQ`                      integer COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `CNA_NO`                        decimal(10, 0) COMMENT '',
    `CERTIFIED_FLAG`                string COMMENT '',
    `DEACTIVATE_DATE`               timestamp COMMENT '',
    `REACTIVATE_DATE`               timestamp COMMENT '',
    `DEACTIVATE_REASON_CODE`        string COMMENT '',
    `COMMENT_TEXT`                  string COMMENT '',
    `USER_DESC`                     string COMMENT '',
    `ACA_CAP_RATING`                integer COMMENT '',
    `UNIT_TYPE`                     string COMMENT '',
    `OPERATION_CAPACITY`            integer COMMENT '',
    `NO_OF_OCCUPANT`                decimal(10, 0) COMMENT '',
    `TRACKING_FLAG`                 string COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
        's3://dpr-raw-zone-test/nomis/agency_internal_locations/_symlink_format_load_only_manifest'
    TBLPROPERTIES (
        'classification' = 'parquet')