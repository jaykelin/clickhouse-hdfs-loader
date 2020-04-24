# clickhouse-hdfs-loader
submit Mapreduce job to hadoop,loading hdfs data to clickhouse，Support text or orc.
## Options
	--additional-cols VAL         : 导入时，数据每行追加内容；多个值以逗号划分 (default: )
	--batch-size N                : batch size load data into clickhouse (default:196608)
	--clickhouse-format VAL       : 导入到Clickhouse的数据格式，一般保留默认配置。支持[TabSeparated|Tab
					SeparatedWithNames|TabSeparatedWithNamesAndTypes|TabSeparatedRaw|
					CSV|CSVWithNames] (default:TabSeparated)
	--connect VAL                 : jdbc:clickhouse://<host>:<port>/<database>
	--daily VAL                   : Deprecated!是否把每天的数据导入到独立的日表 (default: false)
	--daily-expires N             : Deprecated!导入时将会判断过期日表，默认保留3天的日表数据 (default: 3)
	--daily-expires-process VAL   : Deprecated!过期日表的处理规则：
					merge:把过期日表数据导入base表后删除日表，
					drop:把过期日表删除 (default: merge)
	--direct VAL                  : 直接导入目标表，而不经过中间表处理 (default: true)
	--driver VAL                  : 驱动类名，一般保留默认配置 (default: ru.yandex.clickhouse.ClickHouseDriver)
	--dt VAL                      : 导入数据的业务时间,format:YYYY-MM-DD
	--escape-null VAL             : 当设置为false，将会保留\N作为字段值提交给clickhouse (default:true)
	--exclude-fields VAL          : 数据源每行数据，被排除的字段下标，从0开始
	--export-dir VAL              : 数据源HDFS路径
	--extract-hive-partitions VAL : 是否根据数据源路径抽取hive分区，如xxx/dt=2017-01-01/pt=ios，将会抽
					取出两个分区字段，并按顺序追加到每行数据后面。 (default: false)
	--fields-terminated-by VAL    : 数据源文件字段分隔符 (default: |)
	--input-format VAL            : Deprecated!请使用-i参数替代.  (default:
					org.apache.orc.mapreduce.OrcInputFormat)
	--input-split-max-bytes N     : 合并小文件大小阀值，只对-i text 格式的数据源有效 (default:268435456)
	--mapper-class VAL            : Deprecated!请使用-i参数替代.  (default:
					com.kugou.loader.clickhouse.mapper.OrcLoaderMapper)
	--max-tries N                 : 当在导入发生异常时的重试次数 (default: 3)
	--mode VAL                    : <append> 导入时如果日表已存在，数据将会追加到该日表.
					<drop> 导入时如果日表已存在，将会删除后，重建日表. (default: append)
	--num-reduce-tasks N          : 指定reduce task的个数，一般保留默认配置 (default: -1)
	--replace-char VAL            : 替换在字段内容中存在的分隔符关键字，如clickhouse-format=CSV时，分隔符关键
					字为','，字段内容中存在将会被替换 (default:  )
	--table VAL                   : 导入目标表名
	-i VAL                        : 数据源文件存储格式，支持[text|orc]

## Build
`mvn clean package assembly:assembly -DskipTests`  
经过测试，在hadoop 2.7.1， 使用下面的jdbc客户端可以避免http包冲突问题  
https://github.com/jaykelin/clickhouse-jdbc/tree/0.1-SNAPSHOT

## Usage
```bash
hadoop jar clickhouse-hdfs-loader.jar com.kugou.loader.clickhouse.ClickhouseHdfsLoader \
-Dmapreduce.job.queuename=root.default \
-i text \
--connect jdbc:clickhouse://xxx:8123/database \
--username xx --password xx \
--table distributed_table \
--dt 2018-01-01 \
--export-dir hdfs_path  \
--extract-hive-partitions true \
--exclude-fields 0,4,5,8 
```