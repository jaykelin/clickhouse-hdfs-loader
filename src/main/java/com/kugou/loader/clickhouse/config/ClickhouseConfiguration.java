package com.kugou.loader.clickhouse.config;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class ClickhouseConfiguration extends ConfigurationKeys{

    private final static Log log = LogFactory.getLog(ClickhouseConfiguration.class);

    private final static Pattern urlRegexp = Pattern.compile("^jdbc:clickhouse://([\\d\\.\\-_\\w]+):(\\d+)/([\\d\\w\\-_]+)(\\?.+)?$");
    private final static String DEFAULT_DATABASE = "default";

    private Configuration conf;

    public ClickhouseConfiguration(Configuration configuration){
        this.conf = configuration;
    }

    public Configuration getConf(){
        return this.conf;
    }

    public Connection getConnection() throws ClassNotFoundException, SQLException {
        log.info("Clickhouse : Get JDBC Connection for :"+getConnectUrl());
        Class.forName(getDriver());
        return DriverManager.getConnection(getConnectUrl());
    }

    public String getDatabase(){
        String table = conf.get(CLI_P_TABLE);
        String database;
        if(null != table && table.contains(".") && !table.endsWith(".")){
            database = table.substring(0, table.indexOf("."));
        }else{
            Matcher m = urlRegexp.matcher(getConnectUrl());
            if (m.find()) {
                if (m.group(3) != null) {
                    database = m.group(3);
                } else {
                    database = DEFAULT_DATABASE;
                }
            } else {
                throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + getConnectUrl());
            }
        }
        return database;
    }

    public String extractHostFromConnectionUrl(){
        Matcher m = urlRegexp.matcher(getConnectUrl());
        if (m.find()){
            return m.group(1);
        } else {
            return ConfigurationOptions.DEFAULT_CLICKHOUSE_HOST;
        }
    }

    public String getTableName(){
        String table = conf.get(CLI_P_TABLE);
        if(table.contains(".") && !table.endsWith(".")){
            table = table.substring(table.indexOf(".")+1);
        }
        return table;
    }

    public String getDriver(){
        return conf.get(CLI_P_DRIVER);
    }

    public String getConnectUrl(){
        return conf.get(CLI_P_CONNECT);
    }

    public String getClickhouseFormat(){
        return conf.get(CLI_P_CLICKHOUSE_FORMAT);
    }

    public String getReplaceChar(){
        return conf.get(CLI_P_REPACE_CHAR);
    }

    public String getDt(){
        return conf.get(CLI_P_DT);
    }

    public String getNullNonString(){
        return conf.get(CLI_P_NULL_NON_STRING, "");
    }

    public String getNullString(){
        return conf.get(CLI_P_NULL_STRING, "");
    }

    public String getFieldsTerminatedBy(){
        return conf.get(CLI_P_FIELDS_TERMINATED_BY);
    }

    public int getBatchSize(){
        return conf.getInt(CLI_P_BATCH_SIZE, 100);
    }

    public int getMaxTries(){
        return conf.getInt(CLI_P_MAXTRIES, 3);
    }

    public String getTempTablePrefix(){
        return conf.get(LOADER_TEMP_TABLE_PREFIX);
    }

    public int getClickhouseHttpPort(){
        return conf.getInt(CLI_P_CLICKHOUSE_HTTP_PORT, 8123);
    }

    public String get(String name){
        return conf.get(name);
    }

    public int getInt(String name, int defaultValue) {
        return conf.getInt(name, defaultValue);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        return conf.getBoolean(name, defaultValue);
    }

    public int getTargetTableColumnSize(){
        return conf.getInt(CL_TARGET_TABLE_COLUMN_SIZE, 0);
    }

    public boolean isExtractHivePartitions(){
        return conf.getBoolean(ConfigurationKeys.CLI_P_EXTRACT_HIVE_PARTITIONS, false);
    }

    public String getUsername(){
        return conf.get(CLI_P_CLICKHOUSE_USERNAME);
    }

    public String getPassword(){
        return conf.get(CLI_P_CLICKHOUSE_PASSWORD);
    }
}
