package com.kugou.loader.clickhouse.cli;

import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created by jaykelin on 2016/11/1.
 */
public class MainCliParameterParser {

    public CmdLineParser cmdLineParser;

    @Option(name="--connect", required = true, usage = "jdbc:clickhouse://<host>:<port>/<database>")
    public String connect;

    @Option(name="--driver", required = false, usage = "驱动类名，一般保留默认配置")
    public String driver = "ru.yandex.clickhouse.ClickHouseDriver";

    @Option(name="--table", required = true, usage = "导入目标表名")
    public String table;

    @Option(name="--export-dir", required = true, usage = "数据源HDFS路径")
    public String exportDir;

    @Option(name="--fields-terminated-by", required = false, usage = "数据源文件字段分隔符")
    public String fieldsTerminatedBy = "|";

    @Option(name="--null-non-string", required = false, usage = "")
    public String nullNonString = "0";

    @Option(name="--null-string",required = false, usage ="")
    public String nullString = "";

    @Option(name="--clickhouse-format", required = false, usage = "导入到Clickhouse的数据格式，一般保留默认配置。支持[TabSeparated|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TabSeparatedRaw|CSV|CSVWithNames]")
    public String clickhouseFormat = "TabSeparated";

    @Option(name="--replace-char", required = false, usage = "替换在字段内容中存在的分隔符关键字，如clickhouse-format=CSV时，分隔符关键字为','，字段内容中存在将会被替换")
    public String replaceChar = " ";

    @Option(name="--dt",required = true, usage = "导入数据的业务时间,format:YYYY-MM-DD")
    public String dt;

    @Option(name="--batch-size", required = false, usage = "batch size load data into clickhouse")
    public int batchSize = 150000;

    @Option(name="--max-tries", required = false, usage = "当在导入发生异常时的重试次数")
    public int maxTries = 3;

    @Option(name="--num-reduce-tasks", required = false, usage = "指定reduce task的个数，一般保留默认配置")
    public int numReduceTasks = -1;

    @Option(name="--clickhouse-http-port", required = false, usage = "")
    public int clickhouseHttpPort = 8123;

    @Option(name="-i", required = false, usage = "数据源文件存储格式，支持[text|orc]")
    public String i;

    @Option(name="--input-format", required = false, usage = "Deprecated!请使用-i参数替代. ")
    public String inputFormat = "org.apache.orc.mapreduce.OrcInputFormat";

    @Option(name="--mapper-class", required = false, usage = "Deprecated!请使用-i参数替代. ")
    public String mapperClass = "com.kugou.loader.clickhouse.mapper.OrcLoaderMapper";

    @Option(name="--mode", required = false, usage = "<append> 导入时如果日表已存在，数据将会追加到该日表.\n<drop> 导入时如果日表已存在，将会删除后，重建日表.")
    public String mode = "append";

    @Deprecated
    @Option(name="--daily", required = false, usage = "是否把每天的数据导入到独立的日表")
    public String daily = "false";

    @Option(name="--loader-task-executor", required = false, usage = "")
    public int loaderTaskExecute=1;

    @Option(name="--extract-hive-partitions", required = false, usage = "是否根据数据源路径抽取hive分区，如xxx/dt=2017-01-01/pt=ios，将会抽取出两个分区字段，并按顺序追加到每行数据后面。")
    public String extractHivePartitions = "false";

    @Option(name="--daily-expires", required = false, usage = "导入时将会判断过期日表，默认保留3天的日表数据")
    public int dailyExpires = 3;

    @Option(name="--daily-expires-process", required = false, usage = "过期日表的处理规则：merge:把过期日表数据导入base表后删除日表，drop:把过期日表删除")
    public String dailyExpiresProcess = ConfigurationOptions.DailyExpiresProcess.MERGE.toString();

    @Option(name="--exclude-fields", required = false, usage = "数据源每行数据，被排除的字段下标，从0开始")
    public String excludeFieldIndexs = null;

    @Option(name="--username", required = false, usage = "")
    public String username = null;

    @Option(name="--password", required = false, usage = "")
    public String password = null;

    @Option(name="--additional-cols", required = false, usage = "导入时，数据每行追加内容；多个值以逗号划分")
    public String additionalCols = "";

    @Option(name="--direct", required = false, usage = "直接导入目标表，而不经过中间表处理")
    public String direct = "true";

    @Option(name="--help",required = false)
    public boolean help = false;

    @Option(name="--input-split-max-bytes", required = false, usage = "合并小文件大小阀值，只对-i text 格式的数据源有效")
    public Long inputSplitMaxBytes = 268435456l;

    @Option(name="--escape-null", required = false, usage = "当设置为false，将会保留\\N作为字段值提交给clickhouse")
    public String escapeNull = "true";

    public MainCliParameterParser(){
        this.cmdLineParser = new CmdLineParser(this);
    }
}
