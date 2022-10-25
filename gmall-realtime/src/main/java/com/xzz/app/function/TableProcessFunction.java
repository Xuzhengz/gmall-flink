package com.xzz.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @date 2022/10/25-20:44
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject>  {

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {

    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

    }
}