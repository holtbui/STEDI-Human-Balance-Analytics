CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` decimal
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hpb-stedi-project/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');