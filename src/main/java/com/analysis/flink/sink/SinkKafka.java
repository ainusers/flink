package com.analysis.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/6 17:13
 * @description:
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 数据发送到kafka
        dataStream.addSink(new FlinkKafkaProducer011<String>("10.43.80.80:9092","sink-kafka", new SimpleStringSchema()));

        // 打印读取数据
        dataStream.print();

        // 执行任务
        env.execute();
    }
}
