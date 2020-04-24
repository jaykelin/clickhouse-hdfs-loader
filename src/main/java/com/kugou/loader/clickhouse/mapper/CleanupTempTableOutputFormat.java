package com.kugou.loader.clickhouse.mapper;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created by jaykelin on 2017/6/20.
 */
public class CleanupTempTableOutputFormat<K, V> extends OutputFormat<K, V> {
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordWriter<K, V>() {
            public void write(K key, V value) {
            }

            public void close(TaskAttemptContext context) {
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CleanupTempTableOutputCommitter();
    }
}
