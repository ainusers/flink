package com.analysis.flink.window;

import com.analysis.flink.base.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/14 11:21
 * @description: 滚动计数窗口
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class TumblingCount {

    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 将数据扁平化处理
        DataStream<SensorReading> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String s, Collector<SensorReading> collector) throws Exception {
                String[] datas = s.split("");
                collector.collect(new SensorReading(datas[0], Long.getLong(datas[1]), Double.valueOf(datas[2])));
            }
        });

        // 时间滚动窗口
        flatMapStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        })
                .keyBy(data -> data.f0)
                .countWindow(3)
                .maxBy(1);

        // 打印读取数据
        dataStream.print();

        // 执行任务
        env.execute();
    }

}
