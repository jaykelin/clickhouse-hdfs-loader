package com.kugou.loader.clickhouse;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONException;

import java.sql.*;
import java.util.List;

/**
 * Created by jaykelin on 2016/11/25.
 */
public class ClickhouseClient {

    private static final Log log = LogFactory.getLog(ClickhouseClient.class);

    private static final String driver = "ru.yandex.clickhouse.ClickHouseDriver";

    private String host;
    private int port;
    private String username;
    private String password;
    private String database;
    private Connection connection;
    private Statement statement;

    static {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        }
    }

    public ClickhouseClient(String connectionUrl) throws SQLException {
        this(connectionUrl, null, null);
    }

    public ClickhouseClient(String connectionUrl, String username, String password) throws SQLException {

        if(StringUtils.isNotBlank(username)){
            this.connection = DriverManager.getConnection(connectionUrl, username, password);
        }else{
            this.connection = DriverManager.getConnection(connectionUrl);
        }
        this.statement = this.connection.createStatement();
    }

    public ClickhouseClient(String host, int port, String database) throws SQLException {
        this(host, port, database, null, null, driver);
    }

    public ClickhouseClient(String host, int port, String database, String username, String password) throws SQLException {
        this(host, port, database, username, password, driver);
    }

    public ClickhouseClient(String host, int port, String database, String username, String password, String driver) throws SQLException {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;

        String url = "jdbc:clickhouse://"+host+":"+port;
        if(database != null){
            url += "/"+database;
        }

        if(StringUtils.isNotBlank(username)){
            this.connection = DriverManager.getConnection(url, username, password);
        }else{
            this.connection = DriverManager.getConnection(url);
        }
        this.statement = this.connection.createStatement();
    }

    public void createTable(String createTableDDL) throws SQLException {
        this.statement.executeUpdate(createTableDDL);
    }

    public void insert(String sql) throws SQLException {
        this.statement.executeUpdate(sql);
    }

    public int executeUpdate(String sql) throws SQLException {
        return this.statement.executeUpdate(sql);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return this.statement.executeQuery(sql);
    }

    public int dropTableIfExists(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS "+tableName;
        return this.executeUpdate(sql);
    }

    public String queryCreateTableScript(String tableName) throws SQLException {
        String sql = "SHOW CREATE TABLE "+tableName;
        ResultSet ret = this.statement.executeQuery(sql);
        if(ret.next()){
            return ret.getString(1);
        }else{
            throw new SQLException("Cannnot found table "+tableName);
        }
    }

//    public List<String> queryClusterHosts(String cluster) throws SQLException {
//        ResultSet ret = statement.executeQuery("select distinct host_address from system.clusters where cluster='"+cluster+"'  and replica_num = 1");
//        List<String> hosts = Lists.newArrayList();
//        while(ret.next()){
//            hosts.add(ret.getString(1));
//        }
//        ret.close();
//        return hosts;
//    }

    public List<ClusterNodes> queryClusterHosts(String cluster) throws SQLException, JSONException {
        List<ClusterNodes> hosts = Lists.newArrayList();
        if (StringUtils.isNotBlank(cluster)){
            ResultSet ret = statement.executeQuery("select cluster, shard_num, shard_weight, groupArray(host_address) as hosts from system.clusters where cluster='"+cluster+"' group by cluster, shard_num, shard_weight order by shard_num desc");
            while(ret.next()){
                hosts.add(new ClusterNodes(ret.getString(1), ret.getInt(2), ret.getInt(3), ret.getString(4)));
//            hosts.add(ret.getString(1));
            }
            ret.close();
        }
        return hosts;
    }

    public boolean isTableExists(String tableName){
        boolean exists = false;
        try{
            ResultSet ret = statement.executeQuery("SHOW CREATE TABLE "+tableName);
            if(ret.next()){
                if (StringUtils.isNotBlank(ret.getString(1))){
                    exists = true;
                }
            }
        }catch (SQLException e){
            log.warn(e);
        }
        return exists;
    }

    public void close(){
        try{
            if(null != statement){
                statement.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (SQLException e){
            log.warn(e);
        }
    }

    public Connection getConnection(){
        return this.connection;
    }

}
