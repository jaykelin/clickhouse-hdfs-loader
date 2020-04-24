package com.kugou.loader.clickhouse.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jaykelin on 2017/7/18.
 */
public class ClusterNodes {

    private final static boolean DEFAULT_NODE_DATA_STATUS = false;

    private String cluster;
    private Integer shard_num;
    private Integer shard_weight;
    private JSONArray hosts;
    private final String key;
    private Map<String, Boolean> nodeDataStatus = Maps.newLinkedHashMap();


    public ClusterNodes(String hostaddress){
        this.cluster = hostaddress.trim();
        this.shard_num = 1;
        this.shard_weight = 1;
        hosts = new JSONArray();
        hosts.put(hostaddress.trim());
        nodeDataStatus.put(hostaddress.trim(), DEFAULT_NODE_DATA_STATUS);
        this.key = this.cluster+"_shard_num_"+this.shard_num;
    }

    public ClusterNodes(String cluster, int shard_num, Integer shard_weight,
                        String host_address_array_json) throws JSONException {
        this.cluster = cluster;
        this.shard_num = shard_num;
        this.shard_weight = shard_weight;
        hosts = new JSONArray(host_address_array_json);
        for(int i = 0; i< getHostsCount(); i++){
            nodeDataStatus.put(hostAddress(i), DEFAULT_NODE_DATA_STATUS);
        }
        this.key = this.cluster+"_shard_num_"+this.shard_num;
    }

    public String getKey(){
//       return  new StringBuilder(cluster).append(shard_num).hashCode();
        return this.key;
    }

    public String getCluster(){
        return this.cluster;
    }

    public int getShardNum(){
        return this.shard_num;
    }

    public int getShardWeight(){
        return shard_weight;
    }

    public int getHostsCount(){
        return hosts == null ? 0 : hosts.length();
    }

    public String hostAddress(int index) throws JSONException {
        return hosts == null ? null : hosts.getString(index);
    }

    public boolean contains(String hostAddress){
        return nodeDataStatus.containsKey(hostAddress.trim());
    }

    /**
     * 随机去一个副本的地址
     * @return
     * @throws JSONException
     */
    public String getANodeAddress() throws JSONException{
        return hostAddress(new Long(System.currentTimeMillis()%getHostsCount()).intValue());
    }

    public Map<String, Boolean> getNodeDataStatus(){
        return nodeDataStatus;
    }


    public String toString(){
        JSONObject obj =null;
        try {
            obj = new JSONObject();
            obj.put("cluster", this.cluster);
            obj.put("shard_num", this.shard_num);
            obj.put("nodes", this.hosts);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        if (null != obj){
            return obj.toString();
        }
        return "";
    }
}
