package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TradePaymentWindowBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import com.xzz.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/11/26-19:24
 *
 * 交易域支付各窗口汇总表
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
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


        //TODO 读取kafka数据
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "DwsTradePaymentSucWindow";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 转成json
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                collector.collect(jsonObject);
            }
        });

        //TODO 按照订单明细id分组
        KeyedStream<JSONObject, String> orderDetailIdDs = jsObj.keyBy(key -> key.getString("order_detail_id"));

        //TODO 状态编程保留最新数据输出
        SingleOutputStreamOperator<JSONObject> filterDs = orderDetailIdDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<JSONObject> valueState;


            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject value = valueState.value();
                if (value == null) {
                    valueState.update(jsonObject);

                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String currentTs = jsonObject.getString("row_op_ts");
                    String lastTs = value.getString("row_op_ts");

                    int compare = TimestampLtz3CompareUtil.compare(lastTs, currentTs);

                    if (compare != 1) {
                        valueState.update(jsonObject);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });


        //TODO 提取事件时间生成水位线
        SingleOutputStreamOperator<JSONObject> waterDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return DateFormatUtil.toTs(jsonObject.getString("callback_time"), true);
            }
        }));
        //TODO 按照用户id分组
        KeyedStream<JSONObject, String> keyDs = waterDs.keyBy(key -> key.getString("user_id"));

        //TODO 提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> beanDs = keyDs.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastDateTime", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                String lastDate = valueState.value();
                String currentDate = jsonObject.getString("callback_time").split(" ")[0];

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

                if (pay == 1L) {
                    collector.collect(new TradePaymentWindowBean("", "", pay, newPay, null));
                }

            }
        });
        //TODO 开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDs = beanDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean tradePaymentWindowBean, TradePaymentWindowBean t1) throws Exception {
                        tradePaymentWindowBean.setPaymentSucUniqueUserCount(tradePaymentWindowBean.getPaymentSucUniqueUserCount() + t1.getPaymentSucUniqueUserCount());
                        tradePaymentWindowBean.setPaymentSucNewUserCount(tradePaymentWindowBean.getPaymentSucNewUserCount() + t1.getPaymentSucNewUserCount());
                        return tradePaymentWindowBean;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                        TradePaymentWindowBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });

        //TODO 写出ck
        resultDs.print("result::");
        resultDs.addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));


        //TODO 启动
        env.execute();
    }
}