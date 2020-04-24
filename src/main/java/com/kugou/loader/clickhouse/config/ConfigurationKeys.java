package com.kugou.loader.clickhouse.config;

/**
 * Created by jaykelin on 2016/11/28.
 */
public class ConfigurationKeys {

    public final static String CLI_P_CONNECT                    = "connect";
    public final static String CLI_P_CLICKHOUSE_FORMAT          ="clickhouse-format";
    public final static String CLI_P_REPACE_CHAR                = "replace-char";
    public final static String CLI_P_DRIVER                     = "driver";
    public final static String CLI_P_EXPORT_DIR                 = "export-dir";
    public final static String CLI_P_FIELDS_TERMINATED_BY       ="fields-terminated-by";
    public final static String CLI_P_NULL_NON_STRING            = "null-non-string";
    public final static String CLI_P_NULL_STRING                = "null-string";
    public final static String CLI_P_DT                         = "dt";
    public final static String CLI_P_BATCH_SIZE                 = "batch-size";
    public final static String CLI_P_TABLE                      = "table";
    public final static String CLI_P_DATABASE                   = "database";
    public final static String CLI_P_MAXTRIES                   = "max-tries";
    public final static String CLI_P_CLICKHOUSE_HTTP_PORT       = "clickhouse-http-port";
    public final static String CLI_P_LOADER_TASK_EXECUTOR       = "loader-task-executor";
    public final static String CLI_P_EXTRACT_HIVE_PARTITIONS    = "extract-hive-partitions";
    public final static String CLI_P_EXCLUDE_FIELD_INDEXS       = "exclude-field-indexs";
    public final static String CLI_P_CLICKHOUSE_USERNAME        = "username";
    public final static String CLI_P_CLICKHOUSE_PASSWORD        = "password";
    public final static String CLI_P_ADDITIONAL_COLUMNS         = "additional-columns";
    public final static String CLI_P_DIRECT                     = "direct";
    public final static String CLI_P_ESCAPE_NULL                = "escape-null";

    public final static String LOADER_TEMP_TABLE_PREFIX                 = "loader_temp_table_prefix";
    public final static String CL_TARGET_TABLE_DATABASE                 = "cl_target_table_database";
    public final static String CL_TARGET_TABLE_FULLNAME                 = "cl_target_table_fullname";
    public final static String CL_TARGET_TABLE_IS_DISTRIBUTED           = "cl_target_table_is_distributed";
    public final static String CL_TARGET_CLUSTER_NAME                   = "cl_target_cluster_name";
    public final static String CL_TARGET_DISTRIBUTED_SHARDING_KEY       = "cl_target_distributed_sharding_key";
    public final static String CL_TARGET_DISTRIBUTED_SHARDING_KEY_INDEX = "cl_target_distributed_sharding_key_index";
    public final static String CL_TARGET_LOCAL_DATABASE                 = "cl_target_local_database";
    public final static String CL_TARGET_LOCAL_TABLE                    = "cl_target_local_table";
    public final static String CL_TARGET_LOCAL_DAILY_TABLE_FULLNAME     = "cl_target_local_daily_table_fullname";
    public final static String CL_TARGET_TABLE_COLUMN_SIZE              = "cl_target_table_column_size";
    public final static String CL_NUM_REDUCE_TASK                       = "cl_num_reduce_task";
}
