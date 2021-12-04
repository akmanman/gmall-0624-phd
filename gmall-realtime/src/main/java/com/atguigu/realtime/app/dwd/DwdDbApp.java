package com.atguigu.realtime.app.dwd;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.*;

import static com.atguigu.realtime.commont.Constant.*;


public class DwdDbApp extends BaseAppV1 {


    public static void main(String[] args) {
        new DwdDbApp().init(2002,1,"DwdDbApp","DwdDbApp", TOPIC_ODS_Db);
    }


    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 对业务数据进行etl (maxwell-bootstrap同步过来的维度数据tpye类型有"bootstrap"前缀,需要过滤)
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);

        // 2. 读取配置表的数据,做成流(cdc)
        SingleOutputStreamOperator<TableProcess> tpStream = readProcessTable(env);

        // 3. 数据流和配置流进行connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(etlStream, tpStream);

        // 4. 删除不需要sink的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream = filterNoNeedColums(connectedStream);

        // 5. 动态分流, 一个流到kafka, 一个流到hbase
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> twoStreams = dynamicSplit(filteredStream);
        writeToKafka(twoStreams.f0);
        writeToHbase(twoStreams.f1);

    }

    private void writeToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        stream
            .keyBy(t -> t.f1.getSink_table())
            .addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getKafkaSink());
    }

    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicSplit(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>(SINK_TYPE_HBASE) {};
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = stream
                .keyBy(t -> t.f1.getSink_table())
                .process(new KeyedProcessFunction<String, Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {

                    @Override
                    public void processElement(Tuple2<JSONObject, TableProcess> t,
                                               KeyedProcessFunction<String, Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>.Context ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        String sink_type = t.f1.getSink_type();
                        if (SINK_TYPE_KAFKA.equals(sink_type)) {
                            out.collect(t);
                        } else if (SINK_TYPE_HBASE.equals(sink_type)) {
                            ctx.output(hbaseTag, t);
                        }
                    }
                });
        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStream = kafkaStream.getSideOutput(hbaseTag);
        return new Tuple2<>(kafkaStream,hbaseStream);
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNoNeedColums(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        return connectedStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                        JSONObject data = t.f0;
                        List<String> sinkColumns = Arrays.asList(t.f1.getSink_columns().split(","));
//                        Set<Map.Entry<String, Object>> entries = data.entrySet();
//                        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//                        while(iterator.hasNext()){
//                            Map.Entry<String, Object> next = iterator.next();
//                            String key = next.getKey();
//                            if (!sinkColumns.contains(key)){
//                                iterator.remove();
//                            }
//                        }
                        data.keySet().removeIf(key -> !sinkColumns.contains(key));
//                        return Tuple2.of(data,t.f1);
                        return t;
                    }
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                 SingleOutputStreamOperator<TableProcess> tpStream) {

        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>(
                "tpState",
                String.class,
                TableProcess.class
        );
        BroadcastStream<TableProcess> toBcStream = tpStream.broadcast(tpStateDesc);
        
        return dataStream.connect(toBcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                        String key = obj.getString("table") + ":" + obj.getString("type");
                        TableProcess tp = tpState.get(key);
                        if (tp != null) {
                            // 为了简化数据结果, 只保留data数据
                            out.collect(Tuple2.of(obj.getJSONObject("data"),tp));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                        String key  = tp.getSource_table() + ":" + tp.getOperate_type();
                        tpState.put(key,tp);
                    }
                });

    }


    private SingleOutputStreamOperator<TableProcess> readProcessTable(StreamExecutionEnvironment env) {
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create table tp(" +
                        "   source_table string, " +
                        "   operate_type string, " +
                        "   sink_type string, " +
                        "   sink_table string, " +
                        "   sink_columns string, " +
                        "   sink_pk string, " +
                        "   sink_extend string, " +
                        "   primary key(source_table,operate_type) not enforced " +
                        ")with(" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = 'hadoop162',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'aaaaaa',\n" +
                        "  'database-name' = 'gmall2021_realtime',\n" +
                        "  'table-name' = 'table_process'," +
                        "  'scan.startup.mode' = 'initial' " +
                        ")");
        Table tp = tenv.from("tp");
        return tenv.toRetractStream(tp, TableProcess.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .map(obj -> JSON.parseObject(obj.replaceAll("bootstrap-","")))
            .filter(obj -> {
                return obj.getString("database") != null
                        && obj.getString("table") != null
                        && ("insert".equals(obj.getString("type")) || "delete".equals(obj.getString("type")));
            });
    }
}
