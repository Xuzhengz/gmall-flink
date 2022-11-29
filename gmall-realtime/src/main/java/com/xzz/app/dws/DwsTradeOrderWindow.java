package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TradeOrderBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/11/27-19:35
 *
 * 交易域下单各窗口汇总表
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
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


        //TODO 获取kafka数据
        String topic = "dwd_trade_order_detail";
        String groupId = "DwsTradeOrderWindow";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 数据转换为json
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                collector.collect(jsonObject);
            }
        });

        //TODO 按照order_detail_id分组
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


        //TODO 提取时间事件生成水位线
        SingleOutputStreamOperator<JSONObject> waterDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
            }
        }));
        //TODO 按照user_id分组
        KeyedStream<JSONObject, String> keyDs = waterDs.keyBy(key -> key.getString("user_id"));

        //TODO 提取独立下单用户
        SingleOutputStreamOperator<TradeOrderBean> beanDs = keyDs.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastDateTime", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradeOrderBean> collector) throws Exception {
                String lastDate = valueState.value();
                String currentDate = jsonObject.getString("create_time").split(" ")[0];

                long pay = 0L;
                long newPay = 0L;

                if (lastDate == null) {
                    pay = 1L;
                    newPay = 1L;
                    valueState.update(currentDate);
                } else if (!lastDate.equals(currentDate)) {
                    pay = 1L;
                    valueState.update(currentDate);
                }

                Integer count = jsonObject.getInteger("sku_num");
                Double price = jsonObject.getDouble("order_price");

                Double split_activity_amount = jsonObject.getDouble("split_activity_amount");
                Double split_coupon_amount = jsonObject.getDouble("split_coupon_amount");

                if (split_activity_amount == null) {
                    split_activity_amount = 0.0D;
                }
                if (split_coupon_amount == null) {
                    split_coupon_amount = 0.0D;
                }


                collector.collect(new TradeOrderBean(
                        "",
                        "",
                        pay,
                        newPay,
                        split_activity_amount,
                        split_coupon_amount,
                        count * price,
                        null
                ));
            }
        });

        //TODO 开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDs = beanDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean tradeOrderBean, TradeOrderBean tradeOrderBean2) throws Exception {
                        tradeOrderBean.setOrderUniqueUserCount(tradeOrderBean.getOrderUniqueUserCount() + tradeOrderBean2.getOrderUniqueUserCount());
                        tradeOrderBean.setOrderNewUserCount(tradeOrderBean.getOrderNewUserCount() + tradeOrderBean2.getOrderNewUserCount());
                        tradeOrderBean.setOrderOriginalTotalAmount(tradeOrderBean.getOrderOriginalTotalAmount() + tradeOrderBean2.getOrderOriginalTotalAmount());
                        tradeOrderBean.setOrderActivityReduceAmount(tradeOrderBean.getOrderActivityReduceAmount() + tradeOrderBean2.getOrderActivityReduceAmount());
                        tradeOrderBean.setOrderCouponReduceAmount(tradeOrderBean.getOrderCouponReduceAmount() + tradeOrderBean2.getOrderCouponReduceAmount());
                        return tradeOrderBean;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        TradeOrderBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });

        //TODO 写出ck
        resultDs.print(">>>>>>>>>>>>");
        resultDs.addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        //TODO 启动
        env.execute();
    }
}