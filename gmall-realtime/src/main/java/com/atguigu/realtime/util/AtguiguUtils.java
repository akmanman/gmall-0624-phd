package com.atguigu.realtime.util;

import java.util.ArrayList;
import java.util.List;

public class AtguiguUtils {
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
}
