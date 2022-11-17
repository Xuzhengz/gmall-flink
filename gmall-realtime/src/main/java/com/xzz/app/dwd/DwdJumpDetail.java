package com.xzz.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @date 2022/11/3-20:06
 * <p>
 * 流量域-跳出明细表
 */
public class DwdJumpDetail {
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


        // TODO 2. 消费kafka中 dwd_traffic_page_log 主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwdJumpDetail";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // TODO 3.将每行数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDs.map(JSON::parseObject);


        // TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObj.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                System.out.println(jsonObject.getLong("ts"));
                return jsonObject.getLong("ts");
            }
        })).keyBy(data -> data.getJSONObject("common").getString("mid"));

        // TODO 5.定义cep规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).times(2).consecutive().within(Time.seconds(10));
        // TODO 6.规则应用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 7.提取事件（匹配的事件和超时事件）
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> selectDs = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                // pattern 中 begin里的是 key
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });

        DataStream<String> timeoutDs = selectDs.getSideOutput(outputTag);

        // TODO 8.合并两个事件
        DataStream<String> unionDs = selectDs.union(timeoutDs);

        // TODO 9.写出kafka
        selectDs.print("select>>>>>");
        unionDs.print("union>>>>>");
        String sinkTopic = "dwd_traffic_user_jump_detail";
        unionDs.addSink(KafkaUtil.getFlinkKafkaProducer(sinkTopic));

        // TODO 10.启动
        env.execute();

    }
}