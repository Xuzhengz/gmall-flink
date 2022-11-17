package com.xzz.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 徐正洲
 * @create 2022-10-31 16:58
 * <p>
 * 流量域-日志数据分组
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
//    TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        开启ck
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        System.setProperty("HADOOP_USER_NAME","root");
//    TODO 2. 消费topic_log 主题数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));
//    TODO 3. 过滤非json数据并转为json对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });

        DataStream<String> dirtyDs = jsonObjDs.getSideOutput(dirtyTag);
        dirtyDs.print("脏数据--->");

//    TODO 4. 按照mid分组
        KeyedStream<JSONObject, String> midStream = jsonObjDs.keyBy(data -> data.getJSONObject("common").getString("mid"));
//    TODO 5. 使用状态编程新老访客标记的校验

        SingleOutputStreamOperator<JSONObject> jsWithNewFlag = midStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>("state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取is_new标记和时间戳
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                //获取状态中的日期
                String lastDate = state.value();
                if ("1".equals(is_new)) {
                    if (lastDate == null) {
                        state.update(currentDate);
                    } else {
                        if (!lastDate.equals(currentDate)) {
                            jsonObject.getJSONObject("common").put("is_new", "0");
                        }
                    }
                } else {
                    if (lastDate == null) {
                        state.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                    }
                }
                return jsonObject;
            }
        });


//    TODO 6. 使用测输出流分流处理--页面日志为主流，其余为错输出流。
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        SingleOutputStreamOperator<String> pageDs = jsWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //尝试获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    context.output(errorTag, jsonObject.toJSONString());
                }
                //移除错误信息
                jsonObject.remove("err");

                //尝试获取启动信息
                String start = jsonObject.getString("start");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //获取公共信息、页面id、时间戳
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");

                    //尝试获取曝光信息
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历写出
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("pageId", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    //尝试获取动作信息
                    JSONArray actions = jsonObject.getJSONArray("action");
                    if (actions != null && actions.size() > 0) {
                        //遍历写出
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("pageId", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }
                    //移除曝光和动作--页面主流
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }

            }
        });

//    TODO 7. 提取各测输出流数据
        DataStream<String> startDs = pageDs.getSideOutput(startTag);
        DataStream<String> displayDs = pageDs.getSideOutput(displayTag);
        DataStream<String> actionDs = pageDs.getSideOutput(actionTag);
        DataStream<String> errorDs = pageDs.getSideOutput(errorTag);

//    TODO 8. 将数据写入不同的主题
        pageDs.print("pageDs>>>>>>>>");
        startDs.print("startDs>>>>>>>>");
        displayDs.print("displayDs>>>>>>>>");
        actionDs.print("actionDs>>>>>>>>");
        errorDs.print("errorDs>>>>>>>>");
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDs.addSink(KafkaUtil.getFlinkKafkaProducer(page_topic));
        startDs.addSink(KafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDs.addSink(KafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDs.addSink(KafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDs.addSink(KafkaUtil.getFlinkKafkaProducer(error_topic));

//    TODO 9. 启动任务
        env.execute();
    }

}
