package com.analysis.flink.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/6 17:13
 * @description:
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class SinkElasticsearch {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 数据发送到elasticsearch
        List<HttpHost> httpHosts = List.of(new HttpHost("localhost", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<String>(httpHosts,new ElasticsearchMapper()).build());

        // 打印读取数据
        dataStream.print();

        // 执行任务
        env.execute();
    }

    public static class ElasticsearchMapper implements ElasticsearchSinkFunction<String>{
        @Override
        public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 组装数据
            Map<String, String> datas = Map.of("id", s, "name", s);

            // 组织es结构
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("senor")
                    .type("read-data")
                    .source(datas);

            // 发送数据
            requestIndexer.add(indexRequest);
        }
    }
}
