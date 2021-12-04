package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSinkUtil {
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("transaction.timeout.ms",15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(
                "default",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                    @Nullable Long ts) {
                        return new ProducerRecord<>(
                                topic,
                                element.getBytes(StandardCharsets.UTF_8)
                        );
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("transaction.timeout.ms",15 * 60 * 1000);
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> t,
                                                                    @Nullable Long aLong) {
                        JSONObject data = t.f0;
                        String topic = t.f1.getSink_table();
                        return new ProducerRecord<>(
                                topic,
                                data.toJSONString().getBytes(StandardCharsets.UTF_8)
                        );
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
