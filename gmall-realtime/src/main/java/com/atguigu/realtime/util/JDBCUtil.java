package com.atguigu.realtime.util;


import java.sql.Connection;
import java.sql.DriverManager;

import static com.atguigu.realtime.commont.Constant.*;

public class JDBCUtil {
    public static Connection getPhoenixConnection(String phoenixUrl) {
        String phoenixDriver = PHOENIX_DRIVER;
        return getJdbcConnection(phoenixDriver,phoenixUrl);
    }

    private static Connection getJdbcConnection(String driver, String url) {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
