package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @date 2022/11/23-20:51
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) {
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
        //    TODO 2. 获取page页面数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //    TODO 3. 转换json对象，并过滤。
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);

                String page = jsonObject.getJSONObject("page").getString("page_id");

                if ("home".equals(page) || "good_detail".equals(page)) {
                    collector.collect(jsonObject);
                }
            }
        });
        //    TODO 4. 提取事件时间生成watermark
        //    TODO 5. 按mid分组
        //    TODO 6. 使用状态编程过滤出首页与商品详情页的独立访客
        //    TODO 7. 开窗聚合
        //    TODO 8. 写出ck
        //    TODO 9. 启动
    }
}