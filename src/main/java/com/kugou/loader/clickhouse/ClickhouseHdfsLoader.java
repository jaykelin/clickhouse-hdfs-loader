package com.kugou.loader.clickhouse;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.cli.MainCliParameterParser;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import com.kugou.loader.clickhouse.mapper.CleanupTempTableOutputFormat;
import com.kugou.loader.clickhouse.mapper.partitioner.HostSequencePartitioner;
import com.kugou.loader.clickhouse.reducer.ClickhouseLoaderReducer;
import com.kugou.loader.clickhouse.task.OldDailyMergeTask;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.kohsuke.args4j.CmdLineException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/10/29.
 */
public class ClickhouseHdfsLoader extends Configured implements Tool {

    private static final    Log log = LogFactory.getLog(ClickhouseHdfsLoader.class);
    private static final    Pattern CLICKHOUSE_DISTRIBUTED_ENGINE_CAUSE = Pattern.compile("= *Distributed *\\( *'?([A-Za-z0-9_\\-]+)'? *, *'?([A-Za-z0-9_\\-]+)'? *, *'?([A-Za-z0-9_\\-]+)'? *(, *[a-zA-Z0-9_\\-]+\\(([A-Za-z0-9_\\-]+|)\\))? *\\)$");
    private static final    Pattern urlRegexp = Pattern.compile("^jdbc:clickhouse://([\\d\\.\\-_\\w]+):(\\d+)/([\\d\\w\\-_]+)(\\?.+)?$");

    private String          clickhouseClusterName = null;
    private boolean         targetTableIsDistributed = false;
    private String          targetTableDatabase = null;
    private String          targetTableFullName = null;
    private String          targetLocalDatabase = null;
    private String          targetLocalTable    = null;
    private String          targetDistributedTableShardingKey = null;
    private int             targetDistributedTableShardingKeyIndex = ConfigurationOptions.DEFAULT_SHARDING_KEY_INDEX;
    private String          targetLocalDailyTableFullName = null;
    private List<ClusterNodes>    clickhouseClusterHosts = Lists.newArrayList();

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new ClickhouseHdfsLoader(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration(getConf());
        String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();

        MainCliParameterParser cliParameterParser = new MainCliParameterParser();
        try{
            cliParameterParser.cmdLineParser.parseArgument(otherArgs);
        }catch (CmdLineException e){
            cliParameterParser.cmdLineParser.printUsage(System.out);
            return 0;
        }

        if (cliParameterParser.help){
            cliParameterParser.cmdLineParser.printUsage(System.out);
            return 0;
        }

        conf.set(ConfigurationKeys.CLI_P_CLICKHOUSE_FORMAT, cliParameterParser.clickhouseFormat);
        conf.set(ConfigurationKeys.CLI_P_CONNECT, cliParameterParser.connect);
        conf.set(ConfigurationKeys.CLI_P_REPACE_CHAR,cliParameterParser.replaceChar);
        conf.set(ConfigurationKeys.CLI_P_DRIVER,cliParameterParser.driver);
        conf.set(ConfigurationKeys.CLI_P_EXPORT_DIR,cliParameterParser.exportDir);
        conf.set(ConfigurationKeys.CLI_P_FIELDS_TERMINATED_BY,cliParameterParser.fieldsTerminatedBy);
        conf.set(ConfigurationKeys.CLI_P_NULL_NON_STRING,cliParameterParser.nullNonString);
        conf.set(ConfigurationKeys.CLI_P_NULL_STRING,cliParameterParser.nullString);
        conf.set(ConfigurationKeys.CLI_P_DT,cliParameterParser.dt);
        conf.set(ConfigurationKeys.CLI_P_BATCH_SIZE,String.valueOf(cliParameterParser.batchSize));
        conf.set(ConfigurationKeys.CLI_P_TABLE,cliParameterParser.table);
        conf.set(ConfigurationKeys.CLI_P_ADDITIONAL_COLUMNS, cliParameterParser.additionalCols);
        conf.setInt(ConfigurationKeys.CLI_P_MAXTRIES, cliParameterParser.maxTries);
        conf.setInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, cliParameterParser.clickhouseHttpPort);
        conf.setInt(ConfigurationKeys.CLI_P_LOADER_TASK_EXECUTOR, cliParameterParser.loaderTaskExecute);
        conf.setBoolean(ConfigurationKeys.CLI_P_EXTRACT_HIVE_PARTITIONS, BooleanUtils.toBoolean(cliParameterParser.extractHivePartitions, "true", "false"));
        conf.setBoolean(ConfigurationKeys.CLI_P_ESCAPE_NULL, BooleanUtils.toBoolean(cliParameterParser.escapeNull,"true", "false"));
        boolean direct = BooleanUtils.toBoolean(cliParameterParser.direct, "true", "false");
        conf.setBoolean(ConfigurationKeys.CLI_P_DIRECT, direct);
        if(StringUtils.isNotBlank(cliParameterParser.excludeFieldIndexs)){
            conf.set(ConfigurationKeys.CLI_P_EXCLUDE_FIELD_INDEXS, cliParameterParser.excludeFieldIndexs);
        }
        if(StringUtils.isNotBlank(cliParameterParser.username)){
            conf.set(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME, cliParameterParser.username);
        }
        if(StringUtils.isNotBlank(cliParameterParser.password)){
            conf.set(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD, cliParameterParser.password);
        }

        // generate temp table name
        String tempTablePrefix = cliParameterParser.table+"_"+
                cliParameterParser.dt.replaceAll("-","")+"_"+
                System.currentTimeMillis()/1000+"_";
        conf.set(ConfigurationKeys.LOADER_TEMP_TABLE_PREFIX, tempTablePrefix);

        // init clickhouse parameters
        initClickhouseParameters(conf, cliParameterParser);

        log.info("Clickhouse Loader: loading data to clickhouse with direct["+direct+"]");

        if(BooleanUtils.toBoolean(cliParameterParser.daily, "true", "false")){
            String targetTable = cliParameterParser.table;
            if(targetTable.contains(".")){
                targetTable = targetTable.substring(targetTable.indexOf(".")+1);
            }
            // create local daily table
            createTargetDailyTable(conf, StringUtils.isNotBlank(targetLocalTable)?targetLocalTable:targetTable, cliParameterParser.mode);

            Thread mergeWorker = new Thread(new OldDailyMergeTask(conf, cliParameterParser.dailyExpires, cliParameterParser.dailyExpiresProcess, clickhouseClusterHosts));
            mergeWorker.start();
            try  {
                mergeWorker.join() ;
            }  catch  ( InterruptedException e) {
                e.printStackTrace();
            }
        }

        int numReduceTask =  1;
        if(cliParameterParser.numReduceTasks != -1){
            numReduceTask = cliParameterParser.numReduceTasks;
        }else if(targetTableIsDistributed && CollectionUtils.isNotEmpty(clickhouseClusterHosts)){
            int host_cnt = 0;
            for (ClusterNodes n : clickhouseClusterHosts){
                host_cnt += n.getHostsCount();
            }
            numReduceTask = host_cnt * cliParameterParser.loaderTaskExecute;
        }else if(cliParameterParser.loaderTaskExecute >= 1){
            numReduceTask = cliParameterParser.loaderTaskExecute;
        }
        conf.setInt(ConfigurationKeys.CL_NUM_REDUCE_TASK, numReduceTask);
        // 设置task timeout
        conf.set("mapreduce.task.timeout", "1800000");

        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickhouseHdfsLoader.class);
        job.setJobName("Clickhouse HDFS Loader");
        CombineTextInputFormat.setMaxInputSplitSize(job, cliParameterParser.inputSplitMaxBytes);
//        job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "268435456");

        String mapperClass, inputFormatClass;
        if (StringUtils.isNotBlank(cliParameterParser.i)){
            ConfigurationOptions.InputFormats inputFormat = ConfigurationOptions.InputFormats.valueOf(cliParameterParser.i);
            mapperClass = inputFormat.MAPPER_CLAZZ;
            inputFormatClass = inputFormat.INPUT_FORMAT_CLAZZ;
        }else{
            mapperClass = cliParameterParser.mapperClass;
            inputFormatClass = cliParameterParser.inputFormat;
        }

        job.setMapperClass(conf.getClassByName(mapperClass).asSubclass(Mapper.class));
        // 参数配置InputFormat
        job.setInputFormatClass(conf.getClassByName(inputFormatClass).asSubclass(InputFormat.class));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if (direct){
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
        }else{
            job.setReducerClass(ClickhouseLoaderReducer.class);
            job.setNumReduceTasks(numReduceTask);
            job.setOutputFormatClass(CleanupTempTableOutputFormat.class);
        }
//        job.setReducerClass(ClickhouseLoaderReducer.class);
//        job.setOutputFormatClass(NullOutputFormat.class);
//        job.setOutputFormatClass(CleanupTempTableOutputFormat.class);


        //设置Map关闭推测执行task
        job.setMapSpeculativeExecution(false);
        //设置Reduce关闭推测执行task
        job.setReduceSpeculativeExecution(false);

        FileInputFormat.addInputPath(job, new Path(conf.get(ConfigurationKeys.CLI_P_EXPORT_DIR)));

        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counter counter = job.getCounters().findCounter("Clickhouse Loader Counters","Failed records");
        if(null != counter && counter.getValue() > 0){
            log.error("Clickhouse Loader: ERROR! Failed records = "+counter.getValue());
            ret = 1;
        }

        if (!BooleanUtils.toBoolean(cliParameterParser.direct, "true", "false")){
            cleanTemptable(tempTablePrefix, clickhouseClusterHosts, new ClickhouseConfiguration(conf));
        }

        return ret;
    }

    /**
     * init clickhouse parameter
     *
     * @param configuration
     * @param parser
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void initClickhouseParameters(Configuration configuration, MainCliParameterParser parser) throws SQLException, ClassNotFoundException, JSONException {
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(parser.connect,
                configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME),
                configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));

        // target database and table
        // CLI_P_TABLE              -> cli target table
        // CLI_P_DATABASE           -> cli target database
        // CL_TARGET_TABLE_FULLNAME -> cli target database.table

        targetTableDatabase = extractTargetTableDatabase(parser.connect, parser.table);
        configuration.set(ConfigurationKeys.CL_TARGET_TABLE_DATABASE, targetTableDatabase);
        targetTableFullName = parser.table;
        if(!targetTableFullName.contains(".")){
            targetTableFullName = targetTableDatabase + "." + targetTableFullName;
        }
        configuration.set(ConfigurationKeys.CLI_P_DATABASE, targetTableDatabase);
        configuration.set(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME, targetTableFullName);

        log.info("parse and loading parameter:");
        log.info("CLI_P_TABLE              -> "+configuration.get(ConfigurationKeys.CLI_P_TABLE));
        log.info("CLI_P_DATABASE           -> "+configuration.get(ConfigurationKeys.CLI_P_DATABASE));
        log.info("CL_TARGET_TABLE_FULLNAME -> "+configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME));

        String targetCreateDDL = client.queryCreateTableScript(targetTableFullName);
        Matcher m = CLICKHOUSE_DISTRIBUTED_ENGINE_CAUSE.matcher(targetCreateDDL);
        if (m.find()){
            // for Distributed table engine
            // CL_TARGET_CLUSTER_NAME                   -> cluster name
            // CL_TARGET_LOCAL_DATABASE                 -> local database
            // CL_TARGET_LOCAL_TABLE                    -> local table
            // CL_TARGET_DISTRIBUTED_SHARDING_KEY       -> sharding key
            // CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX -> sharding key column index

            targetTableIsDistributed = true;
            clickhouseClusterName = m.group(1);
            targetLocalDatabase = m.group(2);
            targetLocalTable = m.group(3);
            targetDistributedTableShardingKey = m.group(5);

            clickhouseClusterHosts = client.queryClusterHosts(clickhouseClusterName);
            configuration.set(ConfigurationKeys.CL_TARGET_CLUSTER_NAME, clickhouseClusterName);
            configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE, targetLocalDatabase);
            configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_TABLE, targetLocalTable);
            if (StringUtils.isNotBlank(targetDistributedTableShardingKey))
                configuration.set(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY, targetDistributedTableShardingKey);

            // sharding_key index
            targetDistributedTableShardingKeyIndex = getClickhouseDistributedShardingKeyIndex(configuration);
            configuration.setInt(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX, targetDistributedTableShardingKeyIndex);

            log.info("CL_TARGET_CLUSTER_NAME                    -> "+configuration.get(ConfigurationKeys.CL_TARGET_CLUSTER_NAME));
            log.info("CL_TARGET_LOCAL_DATABASE                  -> "+configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE));
            log.info("CL_TARGET_LOCAL_TABLE                     -> "+configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_TABLE));
            log.info("CL_TARGET_DISTRIBUTED_SHARDING_KEY        -> "+configuration.get(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY));
            log.info("CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX  -> "+configuration.get(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX));

            log.info("Clickhouse Loader : cluster["+clickhouseClusterName+"] look at "+clickhouseClusterHosts.size()+" hosts.");
            log.info("Clickhouse Loader : target table["+targetTableFullName+"] is Distributed on ["+clickhouseClusterName+"] by sharding["+targetDistributedTableShardingKey+",index="+targetDistributedTableShardingKeyIndex+"], look at ["+targetLocalDatabase+"."+targetLocalTable+"]");
        }else{
            clickhouseClusterHosts.add(new ClusterNodes(new ClickhouseConfiguration(configuration).extractHostFromConnectionUrl()));
        }

        log.info("Clickhouse Loader : load data to table["+targetTableFullName+"].");
        configuration.setBoolean(ConfigurationKeys.CL_TARGET_TABLE_IS_DISTRIBUTED, targetTableIsDistributed);
    }

    private String extractTargetTableDatabase(String connectionUrl, String targetTable){
        String database;
        if(null != targetTable && targetTable.contains(".") && !targetTable.endsWith(".")){
            database = targetTable.substring(0, targetTable.indexOf("."));
        }else{
            Matcher m = urlRegexp.matcher(connectionUrl);
            if (m.find()) {
                if (m.group(3) != null) {
                    database = m.group(3);
                } else {
                    database = ConfigurationOptions.DEFAULT_DATABASE;
                }
            } else {
                throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + connectionUrl);
            }
        }
        return database;
    }

    private int getClickhouseDistributedShardingKeyIndex(Configuration configuration) throws SQLException, ClassNotFoundException {
        String key = configuration.get(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY);
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
        int ret = -1;
        if (StringUtils.isNotBlank(key)){
            String sql = "describe "+configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME);
            ResultSet resultSet = client.executeQuery(sql);
            int index = -1;
            while(resultSet.next()){
                index ++;
                String line = resultSet.getString(1);
                if (key.equalsIgnoreCase(line.trim())){
                    ret = index;
                    break;
                }
            }
            resultSet.close();
        }
        return ret;
    }

    /**
     * 创建本地日表
     * @param configuration
     * @param targetLocalTable
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void createTargetDailyTable(Configuration configuration, String targetLocalTable, String mode) throws SQLException, ClassNotFoundException, JSONException {
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
        // daily table suffix
        String dailyTableSuffix = "_"+configuration.get(ConfigurationKeys.CLI_P_DT).replaceAll("-","");
        String localDatabase    = (StringUtils.isNotBlank(configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE)) ? configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE) : targetTableDatabase);
        // local daily table name
        String localDailyTableName = targetLocalTable + dailyTableSuffix;

        // local daily table fullname
        targetLocalDailyTableFullName = localDatabase + "." + localDailyTableName;
        // target local target fullname
        String targetLocalTableFullName = localDatabase + "." + targetLocalTable;

        log.info("Clickhouse Loader : query target table["+targetLocalTableFullName+"] DDL");
        String targetLocalTableCreateDDL = client.queryCreateTableScript(targetLocalTableFullName);
        String targetLocalDailyCreateDDL = targetLocalTableCreateDDL.replaceAll(targetLocalTable, localDailyTableName);

        if(targetTableIsDistributed){
            // distributed daily table name
            String targetDistributedDailyTableName      = configuration.get(ConfigurationKeys.CLI_P_TABLE) + dailyTableSuffix;
            String targetDistributedDailyTableFullname  = configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME) + dailyTableSuffix;
            String targetDistributedTableCreateDDL      = client.queryCreateTableScript(configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME));
            String targetDistributedDailyCreateDDL      = targetDistributedTableCreateDDL.replaceAll(configuration.get(ConfigurationKeys.CLI_P_TABLE), targetDistributedDailyTableName);

            for (ClusterNodes host : clickhouseClusterHosts){
                for (int i = 0; i<host.getHostsCount(); i++){
                    client = ClickhouseClientHolder.getClickhouseClient(host.hostAddress(i),
                            configuration.getInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, ConfigurationOptions.DEFAULT_CLICKHOUSE_HTTP_PORT),
                            configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE),
                            configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
                    // 处理日表逻辑
                    createDailyTables(client, host.hostAddress(i), targetLocalTableFullName, targetLocalDailyCreateDDL,
                            targetDistributedDailyTableFullname, targetDistributedDailyCreateDDL, mode, targetTableIsDistributed);
                }
            }
        }else{
            // 处理日表逻辑
            createDailyTables(client, "localhost", targetLocalTableFullName, targetLocalDailyCreateDDL,
                    null, null, mode, targetTableIsDistributed);
        }

        configuration.set(ConfigurationKeys.CL_TARGET_LOCAL_DAILY_TABLE_FULLNAME, targetLocalDailyTableFullName);
    }

    /**
     * 创建日表
     * @param client
     * @param host clickhouse 主机 IP
     * @param targetLocalTableFullName 目标本地表名，xx.xxx
     * @param targetLocalDailyCreateDDL 目标本地表创建脚本
     * @param targetDistributedDailyTableFullname 目标分布式表名，yy.yy
     * @param targetDistributedDailyCreateDDL 目标分布式表创建脚本
     * @param mode 日表处理方式;drop or append
     * @param isDistributed
     * @throws SQLException
     */
    private void createDailyTables(ClickhouseClient client, String host, String targetLocalTableFullName, String targetLocalDailyCreateDDL,
                                   String targetDistributedDailyTableFullname, String targetDistributedDailyCreateDDL, String mode, boolean isDistributed) throws SQLException {
        if (client.isTableExists(targetLocalDailyTableFullName)){
            if (mode.equalsIgnoreCase(ConfigurationOptions.RULE_OF_DROP_DAILY_TABLE)){
                log.info("Clickhouse Loader : host["+host+"] drop table ["+targetLocalTableFullName+"]");
                client.dropTableIfExists(targetLocalDailyTableFullName);
                log.info("Clickhouse Loader : host["+host+"] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
                client.executeUpdate(targetLocalDailyCreateDDL);
            }
        }else{
            log.info("Clickhouse Loader : host["+host+"] create daily table["+targetLocalDailyTableFullName+"] ddl["+targetLocalDailyCreateDDL+"]");
            client.executeUpdate(targetLocalDailyCreateDDL);
        }
        if (isDistributed){
            if (client.isTableExists(targetDistributedDailyTableFullname)){
                if (mode.equalsIgnoreCase(ConfigurationOptions.RULE_OF_DROP_DAILY_TABLE)){
                    log.info("Clickhouse Loader : host["+host+"] drop table ["+targetDistributedDailyTableFullname+"]");
                    client.dropTableIfExists(targetDistributedDailyTableFullname);
                    log.info("Clickhouse Loader : host["+host+"] create daily table["+targetDistributedDailyTableFullname+"] ddl["+targetDistributedDailyCreateDDL+"]");
                    client.executeUpdate(targetDistributedDailyCreateDDL);
                }
            }else{
                log.info("Clickhouse Loader : host["+host+"] create daily table["+targetDistributedDailyTableFullname+"] ddl["+targetDistributedDailyCreateDDL+"]");
                client.executeUpdate(targetDistributedDailyCreateDDL);
            }
        }
    }

//    /**
//     * merge and drop old daily table
//     *
//     * @param dailyExpires
//     * @param dailyExpiresProcess
//     * @deprecated
//     */
//    private void mergeAndDropOldDailyTable(Configuration configuration, int dailyExpires, String dailyExpiresProcess, String targetLocalTable) throws SQLException, ClassNotFoundException, JSONException {
//        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
//        String localDatabase    = (StringUtils.isNotBlank(configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE)) ? configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE) : targetTableDatabase);
//        Calendar cal = Calendar.getInstance();
//            cal.add(Calendar.DAY_OF_MONTH, dailyExpires);
//        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
//        String lastDailyTable = targetLocalTable + "_" + df.format(cal.getTime());
//        String targetTableFullname = localDatabase + "." +targetLocalTable;
//
//
//        if(targetTableIsDistributed){
//            for (ClusterNodes host : clickhouseClusterHosts){
//                for (int i = 0; i<host.getHostsCount(); i++){
//                    client = ClickhouseClientHolder.getClickhouseClient(host.hostAddress(i),
//                            configuration.getInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, ConfigurationOptions.DEFAULT_CLICKHOUSE_HTTP_PORT),
//                            configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE),
//                            configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
//                    ResultSet ret = client.executeQuery("select name from system.tables where database='"+localDatabase+"' and name > '" + lastDailyTable + "'");
//                    List<String> oldDailyTableName = Lists.newArrayList();
//                    while(ret.next()){
//                        String dailyTable = ret.getString(1);
//                        if(StringUtils.isNotBlank(dailyTable)){
//                            oldDailyTableName.add(dailyTable);
//                        }
//                    }
//                    ret.close();
//                    if(CollectionUtils.isEmpty(oldDailyTableName)){
//                        log.info("Clickhouse Loader : Cannot found any old daily table before ["+lastDailyTable+"] for ["+targetTableFullname+"]");
//                        return ;
//                    }
//                    for(String table : oldDailyTableName){
//                        String oldDailyTableFullname = localDatabase +"." +table;
//                        log.info("Clickhouse Loader : process old daily table ["+oldDailyTableFullname+"] on host["+host+"]");
//                        if(ConfigurationOptions.DailyExpiresProcess.MERGE.toString().equals(dailyExpiresProcess)){
//                            log.info("Clickhouse Loader : merge old daily table ["+oldDailyTableFullname+"] to ["+localDatabase+"."+targetLocalTable+"]");
//                            client.executeUpdate("INSERT INTO "+localDatabase+"."+targetLocalTable+" FROM SELECT * FROM "+oldDailyTableFullname);
//                        }
//                        client.dropTableIfExists(oldDailyTableFullname);
//                    }
//                }
//            }
//        }else{
//            ResultSet ret = client.executeQuery("select name from system.tables where database='"+localDatabase+"' and name > '" + lastDailyTable + "'");
//            List<String> oldDailyTableName = Lists.newArrayList();
//            while(ret.next()){
//                String dailyTable = ret.getString(1);
//                if(StringUtils.isNotBlank(dailyTable)){
//                    oldDailyTableName.add(dailyTable);
//                }
//            }
//            ret.close();
//            if(CollectionUtils.isEmpty(oldDailyTableName)){
//                log.info("Clickhouse Loader : Cannot found any old daily table before ["+lastDailyTable+"] for ["+targetTableFullname+"]");
//                return ;
//            }
//            for(String table : oldDailyTableName){
//                String oldDailyTableFullname = localDatabase +"." +table;
//                log.info("Clickhouse Loader : process old daily table ["+oldDailyTableFullname+"].");
//                if(ConfigurationOptions.DailyExpiresProcess.MERGE.toString().equals(dailyExpiresProcess)){
//                    log.info("Clickhouse Loader : merge old daily table ["+oldDailyTableFullname+"] to ["+localDatabase+"."+targetLocalTable+"]");
//                    client.executeUpdate("INSERT INTO "+localDatabase+"."+targetLocalTable+" FROM SELECT * FROM "+oldDailyTableFullname);
//                }
//                client.dropTableIfExists(oldDailyTableFullname);
//            }
//        }
//    }

    public void cleanTemptable(String tempTablePrefix, List<ClusterNodes> clusterHostList, ClickhouseConfiguration conf){
        String sql = "select concat(database,'.', name) as tablename from system.tables where database='temp' and name like '"+tempTablePrefix+"%'";
        for (ClusterNodes nodes : clusterHostList){
            for (int i =0 ; i< nodes.getHostsCount(); i++){
                try{
                    String host = nodes.hostAddress(i);
                    ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(host, conf.getClickhouseHttpPort(),
                            conf.getDatabase(), conf.getUsername(), conf.getPassword());
                    ResultSet ret = client.executeQuery(sql);
                    while (ret.next()){
                        String tempTableName = ret.getString(1);
                        log.info(String.format("Drop temptable[%s] on host[%s].", tempTableName, host));
                        client.dropTableIfExists(tempTableName);
                    }
                    ret.close();
                }catch (SQLException e){
                    log.warn("CleanTemptable failed. ", e);
                } catch (JSONException e) {
                    log.error(e.getMessage(), e);
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(200l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
