package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.mapper.decode.DefaultRowRecordDecoder;
import com.kugou.loader.clickhouse.mapper.record.decoder.OrcRecordDecoder;
import com.kugou.loader.clickhouse.mapper.decode.RecordDecoderConfigurable;
import com.kugou.loader.clickhouse.mapper.decode.RowRecordDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcStruct;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jaykelin on 2016/11/24.
 */
public class OrcLoaderMapper extends AbstractClickhouseLoaderMapper<NullWritable, OrcStruct, Text, Text>{

    private final static Logger logger = LoggerFactory.getLogger(OrcLoaderMapper.class);


    @Override
    public RowRecordDecoder<NullWritable, OrcStruct> getRowRecordDecoder(Configuration config) {
        return new DefaultRowRecordDecoder<>(config, new OrcRecordDecoder(new ClickhouseConfiguration(config)));
    }

//    @Override
//    public String readLine(NullWritable key, OrcStruct value, Context context) {
//        ClickhouseConfiguration clickhouseJDBCConfiguration = new ClickhouseConfiguration(context.getConfiguration());
//        String nullNonString = clickhouseJDBCConfiguration.getNullNonString();
//        String nullString = clickhouseJDBCConfiguration.getNullString();
//        String replaceChar = clickhouseJDBCConfiguration.getReplaceChar();
//        StringBuilder row = new StringBuilder();
//        for(int i = 0; i < value.getNumFields(); i++){
//            if (getExcludeFieldIndexs().contains(i)){
//                continue;
//            }
//            if(i != 0 && row.length() > 0) {
//                row.append(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR);
//            }
//            WritableComparable fieldValue = value.getFieldValue(i);
//            String field;
//            if(null == fieldValue){
//                field = nullString;
//            }else{
//                field = fieldValue.toString();
//                if (i == clickhouseDistributedTableShardingKeyIndex){
//                    clickhouseDistributedTableShardingKeyValue = field;
//                }
//                if(field.equals("\\N")){
//                    field = nullNonString;
//                }else if(field.equalsIgnoreCase("NULL")) {
//                    field = nullString;
//                }else {
//                    field = field.replace(ConfigurationOptions.DEFAULT_RESULT_FIELD_SPERATOR, replaceChar.charAt(0));
//                    field = field.replace('\\', '/');
//                }
//            }
//            row.append(field);
//        }
//
//        return row.toString();
//    }

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
