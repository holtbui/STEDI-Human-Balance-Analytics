CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_trusted` (
  ` customerName` string,
  `email` string,
  `phone` string,
  `birthDay` string,
  `serialNumber` string,
  ` registrationDate` bigint,
  ` lastUpdateDate` bigint,
  ` shareWithResearchAsOfDate` bigint,
  ` shareWithPublicAsOfDate` bigint,
  ` shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hpb-stedi-project/customer/trusted/'
TBLPROPERTIES ('classification' = 'json');