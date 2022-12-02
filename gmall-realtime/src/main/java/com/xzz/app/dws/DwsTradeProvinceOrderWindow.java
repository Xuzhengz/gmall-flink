package com.xzz.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xzz.app.function.DimAsyncFunction;
import com.xzz.bean.TradeProvinceOrderWindow;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import com.xzz.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author 徐正洲
 * @create 2022-12-02 14:44
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> source = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        // TODO 4. 过滤字段不完整数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        return userId != null && sourceTypeName != null;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = this.lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderWindow> javaBeanStream = processedStream.map(
                jsonObj -> {
                    String provinceId = jsonObj.getString("province_id");
                    String orderId = jsonObj.getString("order_id");
                    Double orderAmount = jsonObj.getDouble("split_total_amount");
                    Set<String> orderIds = new HashSet<>();
                    orderIds.add(orderId);
                    Long ts = jsonObj.getLong("ts") * 1000L;

                    TradeProvinceOrderWindow tradeProvinceOrderWindow = TradeProvinceOrderWindow.builder()
                            .provinceId(provinceId)
                            .orderIdSet(orderIds)
                            .orderAmount(orderAmount)
                            .ts(ts)
                            .build();
                    return tradeProvinceOrderWindow;
                }
        );

        // TODO 8. 设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderWindow> withWatermarkStream = javaBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderWindow>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderWindow javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 9. 按照省份 ID 分组
        KeyedStream<TradeProvinceOrderWindow, String> keyedByProIdStream =
                withWatermarkStream.keyBy(TradeProvinceOrderWindow::getProvinceId);

        // TODO 10. 开窗
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowDS = keyedByProIdStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
        ));

        // TODO 11. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.getOrderIdSet().addAll(
                                value2.getOrderIdSet()
                        );
                        value1.setOrderAmount(
                                value1.getOrderAmount() + value2.getOrderAmount()
                        );
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeProvinceOrderWindow> elements, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeProvinceOrderWindow element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setOrderCount((long) element.getOrderIdSet().size());
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 12. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> fullInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>("dim_base_province".toUpperCase()) {

                    @Override
                    public void join(TradeProvinceOrderWindow javaBean, JSONObject jsonObj) {
                        String provinceName = jsonObj.getString("name".toUpperCase());
                        javaBean.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderWindow javaBean) {
                        return javaBean.getProvinceId();
                    }
                },
                60 * 50, TimeUnit.SECONDS
        );

        // TODO 13. 写入到 OLAP 数据库
        fullInfoStream.<TradeProvinceOrderWindow>addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        env.execute();
    }

}
