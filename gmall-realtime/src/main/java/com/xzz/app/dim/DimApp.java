package com.xzz.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.ctc.wstx.io.EBCDICCodec;
import com.google.gson.JsonObject;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @date 2022/10/24-20:00
 */
public class DimApp {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // kakfa的分区数量

//        开启ck
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 2.获取kafka topic_db 创建主流
        String topic = "topic_db";
        String groupId = "Dim_App_1024";
        DataStreamSource<String> KafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤非json数据以及只保留增、改、初始化数据
        SingleOutputStreamOperator<JSONObject> filterJsonStream = KafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if (jsonObject.get("type").equals("insert") || jsonObject.get("type").equals("update") || jsonObject.get("type").equals("bootstrap-insert")) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("非json数据：  " + s);
                }
            }
        });
        //TODO 4.Flink CDC 读取mysql配置信息表，创建配置流

        //TODO 5.将配置流处理为广播流

        //TODO 6.连接主流和配置流

        //TODO 7.处理连接流（根据配置信息处理主流数据）

        //TODO 8.数据写出到Phoenix

    }
}