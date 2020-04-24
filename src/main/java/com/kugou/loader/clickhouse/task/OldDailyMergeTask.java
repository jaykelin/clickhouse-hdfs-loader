package com.kugou.loader.clickhouse.task;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by jaykelin on 2016/12/2.
 */
public class OldDailyMergeTask implements Runnable{

    private static final Log log = LogFactory.getLog(OldDailyMergeTask.class);

    private Configuration configuration;
    private int dailyExpires;
    private String dailyExpiresProcess;
    private List<ClusterNodes> clickhouseClusterHosts = null;

    public OldDailyMergeTask(Configuration configuration, int dailyExpires, String dailyExpiresProcess, List<ClusterNodes> clickhouseClusterHosts){
        this.configuration = configuration;
        this.dailyExpires = dailyExpires;
        this.dailyExpiresProcess = dailyExpiresProcess;
        this.clickhouseClusterHosts = clickhouseClusterHosts;
    }

    @Override
    public void run() {
        String targetTableFullname = configuration.get(ConfigurationKeys.CL_TARGET_TABLE_FULLNAME);
        String targetLocalTable = configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_TABLE);
        if(StringUtils.isBlank(targetLocalTable)){
            targetLocalTable = targetTableFullname.substring(targetTableFullname.indexOf(".")+1);
        }
        try {
            mergeAndDropOldDailyTable(configuration, dailyExpires, dailyExpiresProcess, targetLocalTable, 0, null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    /**
     * merge and drop old daily table
     *
     * @param dailyExpires
     * @param dailyExpiresProcess
     */
    private void mergeAndDropOldDailyTable(Configuration configuration, int dailyExpires, String dailyExpiresProcess, String targetLocalTable, int tries, Throwable cause) throws Exception {
        if(tries >= configuration.getInt(ConfigurationKeys.CLI_P_MAXTRIES, ConfigurationOptions.DEFAULT_MAX_TRIES)){
            throw new Exception("Clickhouse Loader try to merge and drop old daily table failed.", cause);
        }
        try{
            ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT),
                    configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
            String localDatabase    = (StringUtils.isNotBlank(configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE)) ? configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE) : configuration.get(ConfigurationKeys.CL_TARGET_TABLE_DATABASE));
            Calendar cal = Calendar.getInstance();
            cal.setTime((new SimpleDateFormat("yyyy-MM-dd")).parse(configuration.get(ConfigurationKeys.CLI_P_DT)));
            cal.add(Calendar.DAY_OF_MONTH, -dailyExpires);
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            String lastDailyTable = targetLocalTable + "_" + df.format(cal.getTime());
            String targetTableFullname = localDatabase + "." +targetLocalTable;
            boolean targetTableIsDistributed = configuration.getBoolean(ConfigurationKeys.CL_TARGET_TABLE_IS_DISTRIBUTED, false);

            if(targetTableIsDistributed){
                // merge and drop local table
                for (ClusterNodes host : clickhouseClusterHosts){
                    for (int i =0; i< host.getHostsCount(); i++){
                        client = ClickhouseClientHolder.getClickhouseClient(host.hostAddress(i),
                                configuration.getInt(ConfigurationKeys.CLI_P_CLICKHOUSE_HTTP_PORT, ConfigurationOptions.DEFAULT_CLICKHOUSE_HTTP_PORT),
                                configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE),
                                configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
                        ResultSet ret = client.executeQuery("select name from system.tables where database='"+localDatabase+"' and match(name, '"+targetLocalTable+"_\\\\d{8}$') and name < '" + lastDailyTable + "'");
                        List<String> oldDailyTableName = Lists.newArrayList();
                        while(ret.next()){
                            String dailyTable = ret.getString(1);
                            if(StringUtils.isNotBlank(dailyTable)){
                                oldDailyTableName.add(dailyTable);
                            }
                        }
                        ret.close();
                        if(CollectionUtils.isEmpty(oldDailyTableName)){
                            log.info("Clickhouse Loader : Cannot found any old daily table before ["+lastDailyTable+"] for ["+targetTableFullname+"]");
                            return ;
                        }
                        for(String table : oldDailyTableName){
                            String oldDailyTableFullname = localDatabase +"." +table;
                            log.info("Clickhouse Loader : process old daily table ["+oldDailyTableFullname+"] on host["+host+"]");
                            if(ConfigurationOptions.DailyExpiresProcess.MERGE.toString().equals(dailyExpiresProcess)){
                                log.info("Clickhouse Loader : merge old daily table ["+oldDailyTableFullname+"] to ["+localDatabase+"."+targetLocalTable+"]");
                                client.executeUpdate("INSERT INTO "+localDatabase+"."+targetLocalTable+" SELECT * FROM "+oldDailyTableFullname);
                            }
                            client.dropTableIfExists(oldDailyTableFullname);
                            // drop distributed table
                            client.dropTableIfExists(configuration.get(ConfigurationKeys.CL_TARGET_TABLE_DATABASE)+"."+table);
                        }
                    }
                }
            }else{
                ResultSet ret = client.executeQuery("select name from system.tables where database='" + localDatabase + "' and match(name, '"+targetLocalTable+"_\\\\d{8}$') and name <= '" + lastDailyTable + "'");
                List<String> oldDailyTableName = Lists.newArrayList();
                while(ret.next()){
                    String dailyTable = ret.getString(1);
                    if(StringUtils.isNotBlank(dailyTable)){
                        oldDailyTableName.add(dailyTable);
                    }
                }
                ret.close();
                if(CollectionUtils.isEmpty(oldDailyTableName)){
                    log.info("Clickhouse Loader : Cannot found any old daily table before [" + lastDailyTable + "] for [" + targetTableFullname + "]");
                    return ;
                }
                for(String table : oldDailyTableName){
                    String oldDailyTableFullname = localDatabase +"." +table;
                    log.info("Clickhouse Loader : process old daily table ["+oldDailyTableFullname+"].");
                    if(ConfigurationOptions.DailyExpiresProcess.MERGE.toString().equals(dailyExpiresProcess)){
                        log.info("Clickhouse Loader : merge old daily table ["+oldDailyTableFullname+"] to ["+localDatabase+"."+targetLocalTable+"]");
                        client.executeUpdate("INSERT INTO "+localDatabase+"."+targetLocalTable+" SELECT * FROM "+oldDailyTableFullname);
                    }
                    client.dropTableIfExists(oldDailyTableFullname);
                }
            }
        }catch (SQLException e){
            log.warn(e.getMessage());
            Thread.sleep((tries+1)*1000l);
            mergeAndDropOldDailyTable(configuration, dailyExpires, dailyExpiresProcess, targetLocalTable, tries + 1, e);
        }
    }
}
