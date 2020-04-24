# create hive table
```sql
CREATE TABLE `test.t_lzj_test01`(
  `h_lst` string,
  `plat` int,
  `h_appver` int,
  `imei` string,
  `h_id` int,
  `type_id` int,
  `path` bigint,
  `parent_path` bigint,
  `time` string,
  `content` int,
  `row` int,
  `parent_path_name` string,
  `path_name` string,
  `content_chinese` string,
  `action` string,
  `h_plugin0` int,
  `h_plugin1` int,
  `h_plugin2` int,
  `h_plugin3` int,
  `etldate` string,
  `source_type` string,
  `h_did` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- load data to dal.os_nbp_action_list_d
```

# create clickhouse table
```sql
CREATE TABLE test_local.t_lzj_test01 ( 
plat Int8,  
h_appver Int16,  
imei String,  
h_id Int32,  
type_id Int8,  
path Int64,  
parent_path Int64,  
time String,  
parent_path_name String,  
path_name String,  
dt Date,  
source_type Int8,  
h_did String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_local/t_lzj_test01/{shard}', '{replica}') 
PARTITION BY dt 
ORDER BY (dt, h_did, imei) 
SETTINGS index_granularity = 8192

CREATE TABLE test.t_lzj_test01 ( 
plat Int8,  
h_appver Int16,  
imei String,  
h_id Int32,  
type_id Int8,  
path Int64,  
parent_path Int64,  
time String,  
parent_path_name String,  
path_name String,  
dt Date,  
source_type Int8,  
h_did String
) ENGINE = Distributed(kg_bi_cluster, 'test_local', 't_lzj_test01', cityHash64(h_did))
```

# Step2 run hadoop jar
```shell
hadoop jar /data1/upload/clickhouse-hdfs-loader-2.1.1-jar-with-dependencies.jar com.kugou.loader.clickhouse.ClickhouseHdfsLoader \
-Dmapreduce.job.queuename=root.default \
-i text \
--connect jdbc:clickhouse://<clickhouse.host>:8123/test \
--username *** --password *** \
--table t_lzj_test01 \
--dt 2019-05-13 \
--export-dir /user/hive/warehouse/test.db/t_lzj_test01/dt=2019-05-13/* \
--daily false \
--direct true \
--input-split-max-bytes 8589934592 \
--batch-size 200000 \
--exclude-fields 0,9,10,13,14,15,16,17,18
```