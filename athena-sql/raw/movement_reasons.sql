CREATE EXTERNAL TABLE `raw`.`nomis_movement_reasons`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `MOVEMENT_TYPE`                 string COMMENT '',
    `MOVEMENT_REASON_CODE`          string COMMENT '',
    `DESCRIPTION`                   string COMMENT '',
    `OPEN_CONTACT_FLAG`             string COMMENT '',
    `CLOSE_CONTACT_FLAG`            string COMMENT '',
    `ACTIVE_FLAG`                   string COMMENT '',
    `LIST_SEQ`                      integer COMMENT '',
    `UPDATE_ALLOWED_FLAG`           string COMMENT '',
    `EXPIRY_DATE`                   timestamp COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `NOTIFICATION_TYPE`             string COMMENT '',
    `NOTIFICATION_FLAG`             string COMMENT '',
    `BILLING_SERVICE_FLAG`          string COMMENT '',
    `TRANSPORTATION_FLAG`           string COMMENT '',
    `HEADER_STATUS_FLAG`            string COMMENT '',
    `IN_MOVEMENT_TYPE`              string COMMENT '',
    `IN_MOVEMENT_REASON_CODE`       string COMMENT '',
    `ESC_RECAP_FLAG`                string COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT '',
    `UNEMPLOYMENT_PAY`              string COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://dpr-raw-zone-test/nomis/movement_reasons/'
    TBLPROPERTIES (
        'classification' = 'parquet')