package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.app.function.DimAsyncFunction;
import com.xzz.bean.TradeUserSpuOrderBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author 徐正洲
 * @create 2022-11-29 9:42
 */
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
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

        //    TODO  获取kafka数据
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

        SingleOutputStreamOperator<TradeUserSpuOrderBean> beanAsyncDs = AsyncDataStream.unorderedWait(
                beanDs,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getSkuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuId(dimInfo.getString("SPU_ID"));
                        tradeUserSpuOrderBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeUserSpuOrderBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                },
                100,
                TimeUnit.SECONDS);


        //todo 生成watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> watermarksDs = beanAsyncDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                        return tradeUserSpuOrderBean.getTs();
                    }
                }));

        //todo 分组、开窗、聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> keyDs = watermarksDs.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
                return new Tuple4<>(tradeUserSpuOrderBean.getUserId(),
                        tradeUserSpuOrderBean.getTrademarkId(),
                        tradeUserSpuOrderBean.getSpuId(),
                        tradeUserSpuOrderBean.getCategory3Id());
            }
        });

        SingleOutputStreamOperator<TradeUserSpuOrderBean> windowDs = keyDs.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean tradeUserSpuOrderBean, TradeUserSpuOrderBean t1) throws Exception {
                        tradeUserSpuOrderBean.getOrderIdSet().addAll(t1.getOrderIdSet());
                        tradeUserSpuOrderBean.setOrderAmount(tradeUserSpuOrderBean.getOrderAmount() + t1.getOrderAmount());
                        return tradeUserSpuOrderBean;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));


                        collector.collect(next);
                    }
                });


        //todo 补充与分组无关的维度字段--减少与维表的交互次数
        // 关联spu_info表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> spuDs = AsyncDataStream.unorderedWait(
                windowDs,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getSpuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        // 关联TM表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tmDs = AsyncDataStream.unorderedWait(
                spuDs,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getTrademarkId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        // 关联category3表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> category3Ds = AsyncDataStream.unorderedWait(
                tmDs,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory3Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        // 关联category2表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> category2Ds = AsyncDataStream.unorderedWait(
                category3Ds,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory2Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory2Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        // 关联category1表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> category1Ds = AsyncDataStream.unorderedWait(
                category2Ds,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory1Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory1Name(dimInfo.getString("NAME"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        //todo 写出ck
        category1Ds.print("result>>>>>>");
        category1Ds.addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        //todo 启动
        env.execute();

    }
}
