CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_trusted` (
  `user` string,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hpb-stedi-project/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');