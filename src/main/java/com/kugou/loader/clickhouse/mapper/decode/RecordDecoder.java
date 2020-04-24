package com.kugou.loader.clickhouse.mapper.decode;


/**
 * Created by jaykelin on 2017/4/5.
 */
public interface RecordDecoder<K, V> {

    boolean hasNext() ;

    void setRecord(K key, V value);

    String next() ;
}
