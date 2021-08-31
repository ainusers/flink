package com.analysis.flink.base;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: tianyong
 * @time: 2021/8/29 16:23
 * @description:
 */
public class StreamHandle {

    public static void StreamHandle() throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 如果当前方法写在main方法中，可以使用这种方式动态传递host和port
        /*ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");*/


        DataStream<String> inputDataStream = env.socketTextStream("localhost", 17979);


        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new PatchHandle.MyFlatMapper())
                .keyBy(0)
                .sum(1); wordCountDataStream.print().setParallelism(1);

        env.execute();
    }
}
