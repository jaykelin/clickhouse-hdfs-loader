package com.kugou.test;

import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.mapper.AbstractClickhouseLoaderMapper;
import com.kugou.loader.clickhouse.mapper.OrcLoaderMapper;
import com.kugou.loader.clickhouse.mapper.decode.RowRecordDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class Test {

    @org.junit.Test
    public void simpleTest(){
        String table = "default.demo";
        if(table.contains(".")){
            System.out.println(table.substring(table.indexOf(".") + 1));
        }
//        String ddl = "CREATE TABLE default.demo ( dt Date,  name String,  value UInt64) ENGINE = MergeTree(dt, (dt, name), 8192)";
//        ddl = ddl.replace("default.demo","xx.xx");
//        ddl = ddl.substring(0, ddl.indexOf("=")+1);
//        ddl+= " StripeLog";
//        System.out.println(ddl);

//        String taskid = "task_1477024973709_128433_m_000001";
//        System.out.println(taskid.substring(taskid.indexOf("m_")));


        String ddl = "ENGINE = Distributed(perftest_2shards_1replicas, 'default', 'dwf_list_play_d')";
        Pattern pattern = Pattern.compile("= *Distributed *\\( *([A-Za-z0-9_\\-]+) *, *'?([A-Za-z0-9_\\-]+)'? *, *'?([A-Za-z0-9_\\-]+)'? *(, *[a-zA-Z0-9_\\-]+\\(([A-Za-z0-9_\\-]+|)\\))? *\\)$");
        Matcher m = pattern.matcher(ddl);
        if(m.find()){
            System.out.println("Found."+m.groupCount());
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));
            System.out.println(m.group(4));
            System.out.println(m.group(5));
        }

        HashFunction fn = Hashing.murmur3_128();

//        for(int j = 0; j< 100; j++){
//            long l = (long)((Math.random()*9+1)*100);
////            System.out.println(l+":"+fn.hashLong(l).asInt());
//            System.out.println(Math.abs(fn.hashString(UUID.randomUUID().toString()).asInt()) % 3);
//        }

        System.out.println(Math.abs(fn.hashString("1_10.1.0.210").hashCode()) % 3);
        System.out.println(Math.abs(fn.hashString("2_10.1.113.176").hashCode()) % 3);
        System.out.println(Math.abs(fn.hashString("3_10.1.112.63").hashCode()) % 3);

        System.out.println(("1".hashCode() & Integer.MAX_VALUE) % 3);
        System.out.println(("2".hashCode() & Integer.MAX_VALUE) % 3);
        System.out.println(("3".hashCode() & Integer.MAX_VALUE) % 3);


        System.out.println("-----------------");
        String s = "\tsdfwef|\tsdsdsdfe\t";
        System.out.println(s.replace('\t', " ".charAt(0)));
    }

    @org.junit.Test
//    测试从hive抽取数据，写入CH中
    public void extractHivePartition(){
//        Configuration conf = new Configuration();
////        conf.set(ConfigurationKeys.CLI_P_EXPORT_DIR, "/user/hive/warehouse/dsl.db/dwf_list_play_d/dt=2016-10-01/pt=ios");
//        conf.set(ConfigurationKeys.CLI_P_EXPORT_DIR, "/user/hive/warehouse/lmmtmp.db/test_user_orc");
//        final ClickhouseConfiguration configuration = new ClickhouseConfiguration(conf);
//
//        AbstractClickhouseLoaderMapper mapper = new AbstractClickhouseLoaderMapper() {
////            @Override
//            public RowRecordDecoder getRowRecordDecoder(Context context) {
//                return null;
//            }
//
////            @Override
//            public String readLine(Object key, Object value, Context context) {
//                Map<String, String> hivePartitions = this.extractHivePartitions(configuration);
//                for (String k : hivePartitions.keySet()){
//                    System.out.println(k+ "===>" + hivePartitions.get(k));
//                }
//                return null;
//            }
//
////            @Override
//            public void write(String host, String hostIndex, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
//
//            }
//        };
//
//        mapper.readLine(null , null, null);
//
//        Pattern CLICKHOUSE_CONNECT_PATTERN = Pattern.compile("^jdbc:clickhouse://([\\d\\.\\-_\\w]+):(\\d+)/([\\d\\w\\-_]+)(\\?.+)?$");
//        System.out.println(CLICKHOUSE_CONNECT_PATTERN.matcher("jdbc:clickhouse://localhost:8123/dal_crosspt_dist?xx=1&xx=2").matches());
    }

    @org.junit.Test
    public void textFileInputTest(){
        String line = "|pc|爱出发+魔法城堡+青春修炼手册|563|656|656562|-53|-23||||||";
        String s = "|";
        StringTokenizer tokenizer = new StringTokenizer(line, s, false);
        int i = 0;


        int start = 0;
        int end = 0;
        for(int j = 0; j < line.length(); j++){
            if(line.charAt(j) == s.charAt(0)){
                i ++ ;
                end = j;
                System.out.println("==>"+line.substring(start, end));
                start = j+1;
            }
        }
        if (start == line.length()){
            i ++ ;
            System.out.println("==>"+"");
        }else if(start < line.length()){
            i ++ ;
            System.out.println("==>"+line.substring(start));
        }

        System.out.println(i);
    }

    @org.junit.Test
    public void mainTest(){
//        int i = 2;
//        System.out.println(new Long(System.currentTimeMillis()%i).intValue());

        Map<String, Boolean> t = Maps.newHashMap();
        t.put("xx", false);
        t.put("yy", true);
        for (String s : t.keySet()){
            System.out.println(t.size()+"--"+t.get("xx"));
            t.put("xx", true);
        }
//        t.put("xx", true);
//        t.put("xx", false);
        System.out.println(t.size()+"--"+t.get("xx"));


        String s = "xxxx\nzzzz";
        System.out.println(s);
        System.out.println(s.replaceAll("\n", "--"));
    }
}
