package com.xzz.app.function;

import com.alibaba.fastjson.JSONObject;

/**
 * @author 徐正洲
 * @create 2022-12-02 13:55
 */
public interface DimJoinFunction<T> {
    String getKey(T t);

    void join(T t, JSONObject dimInfo);


}
