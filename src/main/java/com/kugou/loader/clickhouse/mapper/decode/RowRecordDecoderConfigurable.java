package com.kugou.loader.clickhouse.mapper.decode;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import com.kugou.loader.clickhouse.context.ClickhouseLoaderContext;
import com.kugou.loader.clickhouse.utils.Tuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.SQLException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by jaykelin on 2017/4/6.
 */
public abstract class RowRecordDecoderConfigurable<K, V> implements RowRecordDecoder<K, V>{

    protected ClickhouseConfiguration config = null;
    private   int                     distributedTableShardingKeyIndex = -1;
    private   int                     cursor = 0;
    private   int                     next_cursor = 0;
    private   RecordDecoderConfigurable recordDecoder = null;
    private   List<Integer>           excludeFieldsIndex = Lists.newArrayList();
    private   int                     target_column_cursor = -1;
    private   ClickhouseLoaderContext loaderContext = null;

    public RowRecordDecoderConfigurable(Configuration configuration, RecordDecoderConfigurable recordDecoder){
        this.recordDecoder = recordDecoder;
        this.config = new ClickhouseConfiguration(configuration);
        this.recordDecoder.setConfiguration(config);
        this.distributedTableShardingKeyIndex = config.getInt(ConfigurationKeys.CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX, ConfigurationOptions.DEFAULT_SHARDING_KEY_INDEX);
        // exclude fields
        String excludeFieldIndexsParameter = configuration.get(ConfigurationKeys.CLI_P_EXCLUDE_FIELD_INDEXS);
        if (StringUtils.isNotBlank(excludeFieldIndexsParameter)){
            StringTokenizer tokenizer = new StringTokenizer(excludeFieldIndexsParameter, ",");
            while (tokenizer.hasMoreTokens()){
                excludeFieldsIndex.add(Integer.valueOf(tokenizer.nextToken()));
            }
        }
        try {
            this.loaderContext = new ClickhouseLoaderContext(this.config);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setRowRecord(K key, V value) {
        this.cursor = 0;
        this.next_cursor = 0;
        this.target_column_cursor = -1;
        this.recordDecoder.setRecord(key, value);
    }

    @Override
    public boolean hasNext() {
        this.cursor = this.next_cursor;
        return null != recordDecoder && recordDecoder.hasNext();
    }

    @Override
    public Tuple.Tuple2<Integer, String> nextTuple() {
        if (this.cursor != this.next_cursor){
            this.cursor = this.next_cursor;
        }
        String value = recordDecoder.next();
        this.next_cursor += 1;
        if (!excludeFieldsIndex.contains(this.cursor)){
            target_column_cursor += 1;
            return Tuple.tuple(target_column_cursor, value);
        }
//        return Tuple.tuple(cursor, recordDecoder.next());
        return null;

    }

    public void setConfiguration(ClickhouseConfiguration configuration){
        this.config = configuration;
    }

    /**
     * 当前字段是否是sharding key
     * @return
     */
    public boolean isDistributedTableShardingKey(){
        return target_column_cursor == distributedTableShardingKeyIndex;
    }

    @Override
    public boolean isExcludedField() {
        return excludeFieldsIndex.contains(this.cursor);
    }

    @Override
    public boolean columnIsStringType() {
        if (null != loaderContext){
            return loaderContext.columnIsStringType(target_column_cursor);
        }
        return false;
    }
}
