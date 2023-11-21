CREATE EXTERNAL TABLE `raw`.`awsdms_status`
(
    `Op`                       string COMMENT '',
    `_timestamp`               string COMMENT '',
    `server_name`              string COMMENT '',
    `task_name`                string COMMENT '',
    `task_status`              string COMMENT '',
    `status_time`              timestamp COMMENT '',
    `pending_changes`          bigint COMMENT '',
    `disk_swap_size`           bigint COMMENT '',
    `task_memory`              bigint COMMENT '',
    `source_current_position`  string COMMENT '',
    `source_current_timestamp` timestamp COMMENT '',
    `source_tail_position`     string COMMENT '',
    `source_tail_timestamp`    timestamp COMMENT '',
    `source_timestamp_applied` timestamp COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://dpr-raw-zone-test/awsdms_status/'
    TBLPROPERTIES (
        'classification' = 'parquet')