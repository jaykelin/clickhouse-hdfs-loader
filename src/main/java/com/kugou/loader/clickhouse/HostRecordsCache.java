package com.kugou.loader.clickhouse;

/**
 * Created by jaykelin on 2016/11/25.
 */
public class HostRecordsCache {

    public StringBuilder records = new StringBuilder();
    public int recordsCount = 0;
    public boolean ready = false;

    public synchronized void reset(){
        records.setLength(0);
        recordsCount = 0;
        ready = false;
    }
}
