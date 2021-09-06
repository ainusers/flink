package com.analysis.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/3 16:10
 * @description: 转换算子
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class Operator {
    public static void main(String[] args) throws Exception{
        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
            new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 1. map转换算子
        DataStream<Integer> mapStream = dataStream.map(
            new MapFunction<String, Integer>() {
                public Integer map(String value) throws Exception {
                    System.out.println("====> "+value);
                    return value.length();
                }
            }
        );

        // 2. flagmap转换算子
        DataStream<String> flatMapStream = dataStream.flatMap(
            new FlatMapFunction<String, String>() {
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] fields = value.split(",");
                    for (String field : fields){
                        out.collect(field);
                    }
                }
            }
        );


        // 3. filter转换算子
        DataStream<String> filterStream = dataStream.filter(
            new FilterFunction<String>() {
                public boolean filter(String value) throws Exception {
                    return value == "1";
                }
            }
        );


        // map-reduce
        DataStream<Integer> mapReduceStream = dataStream.map(
            new MapFunction<String, Integer>() {
                public Integer map(String value) throws Exception {
                    String[] data = value.split(",");
                    return Integer.parseInt(data[0]);
                }
            }
        );


        // 4. keyBy (DataStream → KeyedStream)
        // 逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素， 在内部以 hash 的形式实现的
        KeyedStream<Integer, Tuple> keyedStream = mapReduceStream.keyBy("id");
        // 5. Reduce (KeyedStream → DataStream)
        // 一个分组数据流的聚合操作， 合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果
        DataStream<Integer> reduceStream = keyedStream.reduce(
            new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return Math.min(value1, value2);
                }
            }
        );

        // 6. Split (DataStream → SplitStream)
        // 根据某些特征把一个 DataStream 拆分成两个或者多个 DataStream
        // 7. Select (SplitStream→DataStream： )
        // 从一个 SplitStream中获取一个或者多个
        SplitStream<String> splitStream = dataStream.split(
            new OutputSelector<String>() {
                @Override
                public Iterable<String> select(String value) {
                    return (Integer.parseInt(value) > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
                }
            }
        );
        DataStream<String> highTempStream = splitStream.select("high");
        DataStream<String> lowTempStream = splitStream.select("low");
        DataStream<String> allTempStream = splitStream.select("high", "low");


        // 8. Connect (DataStream → ConnectedStreams)
        // 连接两个保持他们类型的数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(
            new MapFunction<String, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(String value) throws Exception {
                    return new Tuple2<String, Double>(value, 0.0);
                }
            }
        );
        ConnectedStreams<Tuple2<String, Double>, String> connectedStreams = warningStream.connect(lowTempStream);
        // 9. CoMap,CoFlatMap (ConnectedStreams → DataStream)
        // 对 ConnectedStreams 中的每一个 Stream 分别进行 map 和 flatMap 处理
        DataStream<Object> resultStream = connectedStreams.map(
            new CoMapFunction<Tuple2<String,Double>, String, Object>() {
                @Override
                public Object map1(Tuple2<String, Double> value) throws Exception {
                    return new Tuple3<>(value.f0, value.f1, "warning");
                }
                @Override
                public Object map2(String value) throws Exception {
                    return new Tuple2<>(value, "healthy");
                }
            }
        );


        // 10. union (DataStream → DataStream)
        // 对两个或者两个以上的 DataStream 进行 union 操作，产生一个包含所有 DataStream 元素的新 DataStream
        DataStream<String> unionStream = highTempStream.union(lowTempStream);


        // 打印读取数据
        mapStream.print();

        // 执行任务
        env.execute();
    }

}
