package com.kugou.loader.clickhouse.context;

import com.google.common.collect.Maps;
import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by jaykelin on 2018/1/26.
 */
public class ClickhouseLoaderContext {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseLoaderContext.class);

    private ClickhouseConfiguration configuration = null;
    private ClickhouseClient client = null;
    private Map<Integer, Tuple.Tuple2<String,String>> colDataType = Maps.newHashMap();
    private Integer columnSize = -1;

    public ClickhouseLoaderContext(ClickhouseConfiguration configuration) throws SQLException {
        this.configuration = configuration;
        this.client = ClickhouseClientHolder.getClickhouseClient(configuration.get(ConfigurationKeys.CLI_P_CONNECT), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_USERNAME), configuration.get(ConfigurationKeys.CLI_P_CLICKHOUSE_PASSWORD));
        String database ;
        String table;
        if (StringUtils.isBlank(database = getTargetLocalDatabase())){
            logger.warn("can not found target_local_database, will use CLI database to replace.");
            database = configuration.get(ConfigurationKeys.CLI_P_DATABASE);
        }
        if (StringUtils.isBlank(table = getTargetLocalTable())){
            logger.warn("can not found target_local_table, will use CLI table to replace.");
            table = configuration.get(ConfigurationKeys.CLI_P_TABLE);
        }
        String sql = "describe "+ database+"."+table;
        logger.info(sql);
        ResultSet resultSet = client.executeQuery(sql);
        int index = -1;
        try{
            while(resultSet.next()){
                String column_name = resultSet.getString(1);
                String column_datatype = resultSet.getString(2);
                logger.info(column_name +"\t"+column_datatype+"\t"+resultSet.getString(3)+"\t"+resultSet.getString(4));
                index ++;
                colDataType.put(index, Tuple.tuple(column_name, column_datatype));
            }
        }catch(ArrayIndexOutOfBoundsException e){
            logger.error(e.getMessage(), e);
        }

        columnSize = index+1;
    }

    public ClickhouseConfiguration getConfiguration(){
        return configuration;
    }

    /**
     * return Distributed Database
     * @return
     */
    public String getTargetDistributedDatabase(){
        return configuration.get(ConfigurationKeys.CL_TARGET_TABLE_DATABASE);
    }

    /**
     * return Distributed table
     * @return
     */
    public String getTargetDistributedTable(){
        return configuration.get(ConfigurationKeys.CLI_P_TABLE);
    }

    public String getTargetLocalDatabase(){
        return configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE);
    }

    public String getTargetLocalTable(){
        return configuration.get(ConfigurationKeys.CL_TARGET_LOCAL_TABLE);
    }

    public int getColumnSize(){
        return columnSize;
    }

    /**
     * 当前字段是否是字符串类型
     * @param col
     * @return
     */
    public boolean columnIsStringType(int col){
        if (col >= getColumnSize()){
            throw new IllegalArgumentException(String.format("column index[s%] >= columnsize[s%]", col, getColumnSize()));
        }
        Tuple.Tuple2<String, String> tuple = colDataType.get(col);
        if (null != tuple && StringUtils.isNotBlank(tuple._2()) &&
                (tuple._2().equals("String") || tuple._2().equals("Nullable(String)"))
                ){
            return true;
        }
        else {
            return false;
        }
    }
}
