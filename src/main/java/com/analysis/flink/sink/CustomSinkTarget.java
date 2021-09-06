package com.analysis.flink.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author: tianyong
 * @time: 2021/9/6 18:13
 * @description: 自定义编写数据源 (以mysql数据库为例)
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class CustomSinkTarget {
    public static void main(String[] args) throws Exception{

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("from-kafka", new SimpleStringSchema(), properties)
        );

        // 数据发送到自定义sink-mysql
        dataStream.addSink(new MyJdbcSink());

        // 打印读取数据
        dataStream.print();

        // 执行任务
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<String> {
        Connection conn = null;
        PreparedStatement insertStmt = null; PreparedStatement updateStmt = null;
        // 创建数据库连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root", "123456");
            // 创建预编译器，有占位符，可传入参数
            insertStmt = conn.prepareStatement("INSERT INTO table (id, name) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE table SET name = ? WHERE id = ?");
        }
        // 执行sql
        @Override
        public void invoke(String value) throws Exception {
            // 如果update未执行，则执行insert
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value);
                insertStmt.execute();
            }
        }
        // 关闭连接
        @Override
        public void close() throws Exception { insertStmt.close(); updateStmt.close();
            conn.close();
        }
    }



}
