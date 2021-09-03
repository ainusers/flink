package com.analysis.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: tianyong
 * @time: 2021/9/1 22:05
 * @description: 从文件中读取数据
 */
public class FromFile {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 从文件中读取数据
        DataStream<java.lang.String> dataStream = env.readTextFile("D:\\百宝箱\\项目代码\\flink\\src\\main\\resources\\hello.txt");


        // 打印读取数据
        dataStream.print();


        // 执行任务
        env.execute();

    }
}
