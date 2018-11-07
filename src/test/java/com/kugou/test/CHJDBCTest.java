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
        StringBuilder query = new StringBuilder();
//        query.append("Insert into test.t_lzj_test01 FORMAT TabSeparated").append("\n");
//        query.append("2017-01-01").append("\t").append("JK").append("\t").append("\n");
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


//
//        System.out.println(query.toString());
//        client.executeUpdate(query.toString());
    }
}
