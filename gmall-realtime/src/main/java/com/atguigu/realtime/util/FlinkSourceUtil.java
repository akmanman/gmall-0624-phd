package com.atguigu.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkSourceUtil {
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("group.id",groupId);
        props.put("auto.offset.reset","earliest");
        //隔离级别
        props.put("isolation.level","read_committed");
        return new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }
}
