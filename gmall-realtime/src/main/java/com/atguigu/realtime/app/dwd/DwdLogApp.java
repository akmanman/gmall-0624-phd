package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.commont.Constant;
import com.atguigu.realtime.util.AtguiguUtils;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static com.atguigu.realtime.commont.Constant.*;


public class DwdLogApp extends BaseAppV1 {
    private static final String START = "start";
    private static final String PAGE = "page";
    private static final String DISPLAY = "display";


    public static void main(String[] args) {
        new DwdLogApp().init(2001,1,"DwdLogApp","DwdLogApp", TOPIC_ODS_LOG);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

        // 1. 对新老用户字段进行修正
        SingleOutputStreamOperator<JSONObject> validatedStream = distinguishNewOrOld(stream);

        // 2. 对数据流进行分流 : 启动,曝光,页面
        HashMap<String, DataStream<JSONObject>> threeStreams = spitStream(validatedStream);

        // 3. 将数据写入到kafka
        writeThreeStreams(threeStreams);
    }

    private void writeThreeStreams(HashMap<String, DataStream<JSONObject>> threeStreams) {
        threeStreams
                .get(START)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_START));
        threeStreams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_PAGE));
        threeStreams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_DISPLAY));
    }


    private HashMap<String, DataStream<JSONObject>> spitStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("pageTag") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {};
        SingleOutputStreamOperator<JSONObject> startStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               ProcessFunction<JSONObject, JSONObject>.Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        if (obj.containsKey("start")) {
                            out.collect(obj);
                        } else {
                            if (obj.containsKey("page")) {
                                ctx.output(pageTag, obj);
                            }
                            if (obj.containsKey("displays")) {
                                JSONArray displays = obj.getJSONArray("displays");
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.putAll(obj.getJSONObject("common"));
                                    display.put("ts", obj.getLong("ts"));
                                    ctx.output(displayTag, display);
                                }
                            }
                        }
                    }
                });
        HashMap<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START,startStream);
        streams.put(PAGE,startStream.getSideOutput(pageTag));
        streams.put(DISPLAY,startStream.getSideOutput(displayTag));

        return streams;
    }

    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner((vs,ts)->ts)
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                @Override
                public void process(String key,
                                    ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context ctx,
                                    Iterable<JSONObject> elements,
                                    Collector<JSONObject> out) throws Exception {
                    ValueState<Boolean> isFirstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<>("is_new", Boolean.class));
                    if (isFirstWindowState.value() == null) {
                        isFirstWindowState.update(true);
                        List<JSONObject> list = AtguiguUtils.toList(elements);
                        list.sort(Comparator.comparing(o -> o.getLong("ts")));
                        for (int i = 0; i < list.size(); i++) {
                            JSONObject obj = list.get(i);
                            if (i == 0) {
                                obj.getJSONObject("common").put("is_new",1);
                            } else {
                                obj.getJSONObject("common").put("is_new",0);
                            }
                            out.collect(obj);
                        }

                    } else { // 如果不是第一个窗口,所有的is_new都是0
                        for (JSONObject obj : elements) {
                            obj.getJSONObject("common").put("is_new",0);
                            out.collect(obj);
                        }
                    }
                }
            });
    }
}
