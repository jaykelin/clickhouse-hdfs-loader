package com.kugou.loader.clickhouse.config;

import org.apache.commons.lang.StringUtils;

/**
 * Created by jaykelin on 2016/11/4.
 */
public class ConfigurationOptions {

    public static final String MAPPER_MAP_SPECULATIVE_EXECUTION = "mapreduce.map.speculative" ;//设置Map推测执行task
    public static final String REDUCE_MAP_SPECULATIVE_EXECUTION = "reduce.map.speculative" ;//设置Map推测执行task

    public static final int     DEFAULT_CLICKHOUSE_HTTP_PORT = 8123;
    public static final int     DEFAULT_SHARDING_KEY_INDEX   = -1;
    public static final String  DEFAULT_DATABASE = "default";
    public static final String  DEFAULT_TEMP_DATABASE = "temp";
    public static final String  DEFAULT_CLICKHOUSE_HOST = "localhost";
    public static final int     DEFAULT_LOADER_TASK_EXECUTOR = 1;
//    public static final char    DEFAULT_RESULT_FIELD_SPERATOR = '\t';
    public static final char    DEFAULT_RESULT_FIELD_SPERATOR = ',';
    public static final String  DEFAULT_RESULT_NULL_STRING = "";
    public static final String  DEFAULT_RESULT_NULL_NON_STRING = "";
    public static final boolean DEFAULT_EXTRACT_HIVE_PARTITIONS = false;
    public static final int     DEFAULT_MAX_TRIES = 3;

    public static final String  RULE_OF_APPEND_DAILY_TABLE = "append";
    public static final String  RULE_OF_DROP_DAILY_TABLE   = "drop";

    public static final int     DEFAULT_MAX_CONCURRENT_MAP_TASK = 20;


    public enum DailyExpiresProcess{
        MERGE("merge"), DROP("drop");

        private String type;

        DailyExpiresProcess(String type){
            this.type = type;
        }

        @Override
        public String toString() {
            return this.type;
        }
    }

    public enum ClickhouseFormats{

        TabSeparated("TabSeparated"),
        TabSeparatedWithNames("TabSeparatedWithNames"),
        TabSeparatedWithNamesAndTypes("TabSeparatedWithNamesAndTypes"),
        TabSeparatedRaw("TabSeparatedRaw"),
        CSV("CSV"),
        CSVWithNames("CSVWithNames");

        public String FORMAT;
        public String SPERATOR;

        ClickhouseFormats(String format) throws UnsupportedOperationException{
            if ("TabSeparated".equals(format) || "TabSeparatedWithNames".equals(format)
                    || "TabSeparatedWithNamesAndTypes".equals(format) || "TabSeparatedRaw".equals(format)){
                this.FORMAT = format;
                this.SPERATOR = "\t";
            }else if("CSV".equals(format) || "CSVWithNames".equals(format)){
                this.FORMAT = format;
                this.SPERATOR = ",";
            }else{
                throw new UnsupportedOperationException("Unsupported Clickhouse Format："+format);
            }
        }
    }

    public enum InputFormats{
        text("text"),orc("orc");

        public String INPUT_FORMAT_CLAZZ;
        public String MAPPER_CLAZZ;


        InputFormats(String format) throws UnsupportedOperationException{
            if (format.equalsIgnoreCase("text")){
                //INPUT_FORMAT_CLAZZ = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
                INPUT_FORMAT_CLAZZ = "org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat";
                MAPPER_CLAZZ = "com.kugou.loader.clickhouse.mapper.TextLoaderMapper";
            }else if (format.equalsIgnoreCase("orc")){
                INPUT_FORMAT_CLAZZ = "org.apache.orc.mapreduce.OrcInputFormat";
                MAPPER_CLAZZ = "com.kugou.loader.clickhouse.mapper.OrcLoaderMapper";
            }else{
                throw new UnsupportedOperationException("Unsupported input format: "+format);
            }
        }
    }
}
