package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TradeUserSpuOrderBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import java.util.HashSet;

/**
 * @author 徐正洲
 * @create 2022-11-29 9:42
 */
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) {
        //todo 获取执行环境
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

        //    TODO  获取page页面数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //    TODO  转换json对象，并过滤。
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                collector.collect(jsonObject);
            }
        });
        //todo order_detail_id唯一键分组去重
        KeyedStream<JSONObject, String> orderDetailIdDs = jsObj.keyBy(key -> key.getString("id"));

        //TODO 数据去重，只取第一条
        SingleOutputStreamOperator<JSONObject> filterDs = orderDetailIdDs.filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("time", String.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
                valueStateDescriptor.enableTimeToLive(ttl);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String value = valueState.value();
                if (value == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        //todo 转换成javabean
        SingleOutputStreamOperator<TradeUserSpuOrderBean> beanDs = filterDs.map(line -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(line.getString("order_id"));

            return TradeUserSpuOrderBean.builder()
                    .skuId(line.getString("sku_id"))
                    .userId(line.getString("user_id"))
                    .orderAmount(line.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(line.getString("create_time"), true))
                    .build();
        });

        //todo 补充与分组相关的维度信息，关联sku_info维表信息。
        SingleOutputStreamOperator<TradeUserSpuOrderBean> map = beanDs.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建phonenix连接池
            }

            @Override
            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {


                return null;
            }
        });


        //todo 生成watermark
        //todo 分组开窗
        //todo 补充与分组无关的维度字段--减少与维表的交互次数
        //todo 写出ck
        //todo 启动

    }
}
