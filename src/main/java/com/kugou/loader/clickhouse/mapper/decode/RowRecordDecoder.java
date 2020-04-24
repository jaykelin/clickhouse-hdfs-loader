package com.kugou.loader.clickhouse.mapper.decode;

import com.kugou.loader.clickhouse.utils.Tuple;

/**
 * Created by jaykelin on 2017/4/6.
 */
public interface RowRecordDecoder<K, V> {

    void setRowRecord(K key, V value);

    boolean hasNext();

    Tuple.Tuple2<Integer, String> nextTuple();

    boolean isDistributedTableShardingKey();

    boolean isExcludedField();

    /**
     * 字段是否字符串类型
     * @return
     */
    boolean columnIsStringType();
}
