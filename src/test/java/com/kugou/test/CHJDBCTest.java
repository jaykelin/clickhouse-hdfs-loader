package com.kugou.test;

import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by jaykelin on 2017/4/18.
 */
public class CHJDBCTest {

    @org.junit.Test
    public void insertTest() throws SQLException, JSONException {
//        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient("10.12.0.22", 38123, "default", "default", "default@kugou");
        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient("10.113.1.90", 8123, "default", "hadoop", "hadoop");
        ResultSet ret = client.executeQuery("select cluster, shard_num, groupArray(host_address) from system.clusters group by cluster, shard_num");
        while (ret.next()){
            System.out.println(ret.getString(1));
            System.out.println(ret.getInt(2));
            String hosts = ret.getString(3);
            System.out.println(hosts);
            JSONArray array = new JSONArray(hosts);
            System.out.println(array.getString(0));
            System.out.println("----------------");
        }

    //插入某条数据
// CREATE TABLE lmmtmp.test_user2 ( user_id Int64,  user_name Nullable(String),  created_date Date) ENGINE = MergeTree(created_date, user_id, 8192)
        StringBuilder query = new StringBuilder();
        query.append("Insert into lmmtmp.test_user2 FORMAT TabSeparated").append("\n");
        query.append("109999").append("\t").append("").append("\t").append("2018-11-08").append("\n");
        System.out.println(query.toString());
        client.executeUpdate(query.toString());
    }
}
