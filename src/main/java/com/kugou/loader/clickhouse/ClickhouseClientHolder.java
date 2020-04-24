package com.kugou.loader.clickhouse;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ru.yandex.clickhouse.util.apache.StringUtils;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/25.
 */
public class ClickhouseClientHolder {

    private static final Log log = LogFactory.getLog(ClickhouseClientHolder.class);

    private static Map<String, ClickhouseClient> clientHolders = Maps.newHashMap();
    private static Pattern CLICKHOUSE_CONNECT_PATTERN = Pattern.compile("^jdbc:clickhouse://([\\d\\.\\-_\\w]+):(\\d+)/([\\d\\w\\-_]+)(\\?.+)?$");

    public static synchronized ClickhouseClient getClickhouseClient(String host, int port, String database) throws SQLException {
        String key = "jdbc:clickhouse://"+host+":"+port+"/"+database;
        if(!clientHolders.containsKey(key)){
            clientHolders.put(key, new ClickhouseClient(host, port, database));
        }
        return clientHolders.get(key);
    }

    public static synchronized ClickhouseClient getClickhouseClient(String host, int port, String database, String username, String password) throws SQLException {
        if(StringUtils.isBlank(username) || StringUtils.isBlank(password)){
            return getClickhouseClient(host, port, database);
        }
        String key = "jdbc:clickhouse://"+username+":"+password+"@"+host+":"+port+"/"+database;
        if(!clientHolders.containsKey(key)){
            clientHolders.put(key, new ClickhouseClient(host, port, database, username, password));
        }
        return clientHolders.get(key);
    }

    public static synchronized ClickhouseClient getClickhouseClient(String connectionUrl) throws SQLException {
        if(!clientHolders.containsKey(connectionUrl)){
            clientHolders.put(connectionUrl, new ClickhouseClient(connectionUrl));
        }
        return clientHolders.get(connectionUrl);
    }

    public static synchronized ClickhouseClient getClickhouseClient(String connectionUrl, String username, String password) throws SQLException {
        log.info("Clickhouse Loader : get clickhouse client["+connectionUrl+"] for user="+username);
        if(StringUtils.isBlank(username) || StringUtils.isBlank(password)){
            return getClickhouseClient(connectionUrl);
        }
        Matcher m = CLICKHOUSE_CONNECT_PATTERN.matcher(connectionUrl);
        if(!m.matches()){
            throw new IllegalArgumentException("Cannot parse jdbc connect : "+connectionUrl);
        }
        String key = "jdbc:clickhouse://"+username+":"+password+"@"+m.group(1)+":"+m.group(2)+"/"+m.group(3);
        if(!clientHolders.containsKey(key)){
            clientHolders.put(key, new ClickhouseClient(connectionUrl, username, password));
        }
        return clientHolders.get(key);
    }
}
