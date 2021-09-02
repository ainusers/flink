package com.analysis.flink.env;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: tianyong
 * @time: 2021/9/2 10:55
 * @description: 创建批处理、流处理、本地运行环境
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class DefaultEnv {

    public static StreamExecutionEnvironment instance() {

        // 创建批处理运行环境
        ExecutionEnvironment envPatch = ExecutionEnvironment.getExecutionEnvironment();

        // 创建流处理运行环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();

        // 本地模式 (可以查看浏览器控制台页面)
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8888);
        StreamExecutionEnvironment envLocal = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        return null;
    }






}
