package com.kugou.loader.clickhouse.mapper.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by jaykelin on 2016/11/29.
 */
public class HostSequencePartitioner extends HashPartitioner<Text, Text>{

    public HostSequencePartitioner(){
        super();
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String k = key.toString();
        int index = k.indexOf("@");
        if (index > -1){
            key = new Text(k.substring(0, index));
        }
        return super.getPartition(key, value, numReduceTasks);
    }
}
