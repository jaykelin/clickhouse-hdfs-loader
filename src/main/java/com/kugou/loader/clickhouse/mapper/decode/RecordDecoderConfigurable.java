package com.kugou.loader.clickhouse.mapper.decode;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;

/**
 * Created by jaykelin on 2017/4/6.
 */
public abstract class RecordDecoderConfigurable<K, V> implements RecordDecoder<K, V>{

    protected ClickhouseConfiguration configuration = null;

    public RecordDecoderConfigurable(ClickhouseConfiguration configuration){
        this.configuration = configuration;
    }

    public void setConfiguration(ClickhouseConfiguration configuration){
        this.configuration = configuration;
    }
}
