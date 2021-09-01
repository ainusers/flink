package com.analysis.flink.source;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;

/**
 * @author: tianyong
 * @time: 2021/9/1 22:05
 * @description: 从集合中读取数据
 */
public class FromCollection {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 从集合中读取数据
        DataStream<Integer> dataStream = env.fromCollection(Arrays.asList(2, 1, 3));


        // 打印读取数据
        dataStream.print();


        // 执行任务
        env.execute();

    }
}
