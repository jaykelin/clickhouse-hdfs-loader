package com.kugou.loader.clickhouse.mapper;

import com.google.common.collect.Lists;
import com.kugou.loader.clickhouse.ClickhouseClient;
import com.kugou.loader.clickhouse.ClickhouseClientHolder;
import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.config.ConfigurationKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by jaykelin on 2017/6/20.
 */
public class CleanupTempTableOutputCommitter extends OutputCommitter{

    private static final Log logger = LogFactory.getLog(CleanupTempTableOutputCommitter.class);

    private List<ClusterNodes> clusterNodes = Lists.newArrayList();

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
        logger.info("Setup job for id="+jobContext.getJobID().toString());
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
        logger.info("Setup task for id="+taskAttemptContext.getTaskAttemptID().getTaskID().toString());
        ClickhouseConfiguration config = new ClickhouseConfiguration(taskAttemptContext.getConfiguration());
        String clickhouseClusterName = config.get(ConfigurationKeys.CL_TARGET_CLUSTER_NAME);
        try{
            ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(config);
            clusterNodes = client.queryClusterHosts(clickhouseClusterName);
        } catch (SQLException e){
            throw new IOException(e.getMessage(), e);
        } catch (JSONException e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
        logger.info("Commit task for id="+taskAttemptContext.getTaskAttemptID().getTaskID().toString());
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
        logger.info("Abort task for id="+taskAttemptContext.getTaskAttemptID().getTaskID().toString());
        ClickhouseConfiguration config = new ClickhouseConfiguration(taskAttemptContext.getConfiguration());
        String taskId = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
        if (-1 < taskId.indexOf("m_")){
            String tempTable = config.getTempTablePrefix()+taskId.substring(taskId.indexOf("m_"))+"_"+taskAttemptContext.getTaskAttemptID().getId();
            try{
                for(ClusterNodes nodes : clusterNodes){
                    for (int i =0; i< nodes.getHostsCount(); i++){
                        String host = nodes.hostAddress(i);
                        ClickhouseClient client = ClickhouseClientHolder.getClickhouseClient(host, config.getClickhouseHttpPort(),
                                config.get(ConfigurationKeys.CL_TARGET_LOCAL_DATABASE), config.getUsername(), config.getPassword());
                        logger.info(String.format("Drop temptable[%s] on host[%s] for abortTask.", tempTable, host));
                        client.dropTableIfExists(tempTable);
                    }

                }
            } catch (SQLException e){
                logger.warn(String.format("Clean task failed!temptable[%s] maybe retained.", tempTable), e);
            } catch (JSONException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }

    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
        logger.info("Abort job for id="+jobContext.getJobID().toString());
        super.abortJob(jobContext, state);
    }
}
