package com.kugou.loader.clickhouse.mapper.record.decoder;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.mapper.decode.RecordDecoderConfigurable;
import org.apache.hadoop.io.Text;

/**
 * Created by jaykelin on 2017/4/5.
 */
public class TextRecordDecoder extends RecordDecoderConfigurable<Object, Text> {

    private String record = null;
    int index = 0, start = 0, end = 0;

    public TextRecordDecoder(ClickhouseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean hasNext() {
        return null != record && start <= record.length();
    }

    @Override
    public void setRecord(Object key, Text value) {
        this.record = value.toString();
        this.index = 0; this.start = 0; this.end = 0;
    }

    @Override
    public String next() {
        String field = null;
        for(int i = start; i < record.length(); i++){
            end = i;
            if(record.charAt(i) == configuration.getFieldsTerminatedBy().charAt(0)){
                field = record.substring(start, i);
                start = end + 1;
                break;
            }
        }
        if (null == field && start <= record.length()){
            field = record.substring(start);
            start = record.length() + 1;
        }
        return field;
    }
}
