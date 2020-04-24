package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.mapper.decode.*;
import com.kugou.loader.clickhouse.mapper.record.decoder.TextRecordDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;

/**
 * Created by jaykelin on 2016/11/29.
 */
public class TextLoaderMapper extends AbstractClickhouseLoaderMapper<Object, Text, Text, Text> {

    private static final Log logger = LogFactory.getLog(TextLoaderMapper.class);

    @Override
    public RowRecordDecoder getRowRecordDecoder(Configuration config) {
        return new DefaultRowRecordDecoder<>(config, new TextRecordDecoder(new ClickhouseConfiguration(config)));
    }

    @Override
    public void write(ClusterNodes nodes, String mapTaskIdentify, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        for(String host: nodes.getNodeDataStatus().keySet()){
            if (nodes.getNodeDataStatus().get(host)){
                if(!tempTable.contains(".")){
                    tempTable = tempDatabase + "." + tempTable;
                }
                logger.info("Output result: "+mapTaskIdentify+"@"+host+"-->"+tempTable);
                context.write(new Text(mapTaskIdentify+"@"+host), new Text(tempTable));
            }
        }

//        try {
//            for (int i = 0; i< nodes.getHostsCount(); i++){
//                String host = nodes.hostAddress(i);
//                if(!tempTable.contains(".")){
//                    tempTable = tempDatabase + "." + tempTable;
//                }
//                logger.info("Output result: "+mapTaskIdentify+"@"+host+"-->"+tempTable);
//                context.write(new Text(mapTaskIdentify+"@"+host), new Text(tempTable));
//            }
//        } catch (JSONException e) {
//            logger.error(e.getMessage(), e);
//            e.printStackTrace();
//        }
    }
}
