package com.atguigu.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.commont.Constant;
import com.atguigu.realtime.util.JDBCUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    private Connection phoenixConn;

    @Override
    public void close() throws Exception {
        if (phoenixConn != null){
            phoenixConn.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取键控状态 初始化Phoenix jdbc连接

        phoenixConn = JDBCUtil.getPhoenixConnection(Constant.PHOENIX_URL);

    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 实现具体的写入逻辑
        TableProcess tableProcess = value.f1;
        checkTable(tableProcess);
    }

    private void checkTable(TableProcess tableProcess) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql
            .append("create table if not exists ")
            .append(tableProcess.getSink_table())
            .append("(")
            .append(tableProcess.getSink_columns().replaceAll(","," varchar, "))
            .append(" varchar, constraint pk_name primary key(")
            .append(tableProcess.getSink_pk() == null ? "id" : tableProcess.getSink_pk())
            .append("))");

        PreparedStatement ps = phoenixConn.prepareStatement(sql.toString());
        ps.execute();
        phoenixConn.commit();
        ps.close();
    }
}
