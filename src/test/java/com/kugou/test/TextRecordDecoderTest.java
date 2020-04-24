package com.kugou.test;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.mapper.decode.DefaultRowRecordDecoder;
import com.kugou.loader.clickhouse.mapper.record.decoder.TextRecordDecoder;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Created by jaykelin on 2017/4/6.
 */
public class TextRecordDecoderTest {

    @Test
    public void decoderTest(){
        Configuration conf = new Configuration();
        conf.set(ConfigurationKeys.CLI_P_FIELDS_TERMINATED_BY, "|");
        TextRecordDecoder recordDecoder = new TextRecordDecoder(new ClickhouseConfiguration(conf));

        String nullString = "", nullNonString="";

        DefaultRowRecordDecoder rowRecordDecoder = new DefaultRowRecordDecoder<>(conf, recordDecoder);
        rowRecordDecoder.setRowRecord(null, new Text("2017-04-16|pc|弹幕|7575|8417|0|0|0|0|0|0|\\N"));
        while(rowRecordDecoder.hasNext()){
            Tuple.Tuple2 tuple2 = rowRecordDecoder.nextTuple();
            String field;
            if (null == tuple2._2()){
                field = nullString;
            }
            else if (StringUtils.equals(tuple2._2().toString(), "\\N")){
                field = nullNonString;
            }
            else {
                field = tuple2._2().toString().replace('\t', ' ').replace('\\', '/');
            }
            System.out.println(tuple2._1()+":"+field);
        }
    }
}
