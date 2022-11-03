package com.xzz.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @create 2022-11-01 17:37
 *
 * 流量域-日活明细表
 */
public class DwdUvDetail {
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


        //    TODO 2. 消费kafka中 dwd_traffic_page_log 主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwdUvDetail";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // TODO 3.过滤掉上一跳页面不为null的数据并将每行数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据：" + s);
                }
            }
        });
        // TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDs.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 5.状态编程实现mid去重
        SingleOutputStreamOperator<JSONObject> uvDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> tsState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("tsState", String.class);

                // 使用TTL优化状态,一天后若无创建和写入则删除状态，释放资源。
                tsState = getRuntimeContext().getState(valueStateDescriptor);
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
            }



            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastDate = tsState.value();
                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(currentDate)) {
                    tsState.update(currentDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // TODO 6.将数据写入kafka
        uvDs.print(">>>>>>>");

        String tarTopic = "dwd_traffic_unique_visitor_detail";
        uvDs.map(json -> json.toJSONString())
                .addSink(KafkaUtil.getFlinkKafkaProducer(tarTopic));

        // TODO 7.启动
        env.execute();
    }
}
