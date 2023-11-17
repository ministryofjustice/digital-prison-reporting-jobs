CREATE EXTERNAL TABLE `dms_raw`.`oms_owner_offender_external_movements_load_only`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `OFFENDER_BOOK_ID`              decimal(10, 0) COMMENT '',
    `MOVEMENT_SEQ`                  integer COMMENT '',
    `MOVEMENT_DATE`                 timestamp COMMENT '',
    `MOVEMENT_TIME`                 timestamp COMMENT '',
    `INTERNAL_SCHEDULE_TYPE`        string COMMENT '',
    `INTERNAL_SCHEDULE_REASON_CODE` string COMMENT '',
    `MOVEMENT_TYPE`                 string COMMENT '',
    `MOVEMENT_REASON_CODE`          string COMMENT '',
    `DIRECTION_CODE`                string COMMENT '',
    `ARREST_AGENCY_LOC_ID`          string COMMENT '',
    `TO_PROV_STAT_CODE`             string COMMENT '',
    `ESCORT_CODE`                   string COMMENT '',
    `FROM_AGY_LOC_ID`               string COMMENT '',
    `TO_AGY_LOC_ID`                 string COMMENT '',
    `ACTIVE_FLAG`                   string COMMENT '',
    `ESCORT_TEXT`                   string COMMENT '',
    `COMMENT_TEXT`                  string COMMENT '',
    `REPORTING_DATE`                timestamp COMMENT '',
    `TO_CITY`                       string COMMENT '',
    `FROM_CITY`                     string COMMENT '',
    `REPORTING_TIME`                timestamp COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `EVENT_ID`                      decimal(10, 0) COMMENT '',
    `PARENT_EVENT_ID`               decimal(10, 0) COMMENT '',
    `TO_COUNTRY_CODE`               string COMMENT '',
    `OJ_LOCATION_CODE`              string COMMENT '',
    `APPLICATION_DATE`              timestamp COMMENT '',
    `APPLICATION_TIME`              timestamp COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT '',
    `TO_ADDRESS_ID`                 decimal(10, 0) COMMENT '',
    `FROM_ADDRESS_ID`               decimal(10, 0) COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
        's3://dpr-dms-raw-zone-development/OMS_OWNER/OFFENDER_EXTERNAL_MOVEMENTS/_symlink_format_load_only_manifest'
    TBLPROPERTIES (
        'classification' = 'parquet')