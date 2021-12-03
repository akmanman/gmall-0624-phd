package com.atguigu.realtime.app.dwd;

import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DwdLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdLogApp().init(2001,1,"DwdLogApp","DwdLogApp","ods_log");
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

    }
}
