package com.analysis.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/6 17:13
 * @description:
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class SinkRedis {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 数据发送到redis
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        dataStream.addSink(new RedisSink<>(flinkJedisPoolConfig,new RedisMapper()));

        // 打印读取数据
        dataStream.print();

        // 执行任务
        env.execute();
    }

    public static class RedisMapper implements org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper<String>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }
        @Override
        public String getKeyFromData(String s) {
            return s;
        }
        @Override
        public String getValueFromData(String s) {
            return s;
        }
    }
}
