package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.CartAddUuBean;
import com.xzz.bean.UserRegisterBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.formatter.FormatUtil;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-11-25 17:22
 */
public class DwsTradeCartAddUuWindow {
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

        //    TODO 2. 获取page页面数据
        String topic = "dwd_trade_cart_add";
        String groupId = "DwsTradeCartAddUuWindow";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //    TODO 3. 转换json对象，并过滤。
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                collector.collect(jsonObject);
            }
        });

        //    TODO 4. 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> watermarkDs = jsObj.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String create_time = jsonObject.getString("create_time");
                String operate_time = jsonObject.getString("operate_time");
                if (operate_time != null) {
                    return DateFormatUtil.toTs(operate_time, true);
                } else {
                    return DateFormatUtil.toTs(create_time, true);
                }
            }
        }));

        //    TODO 5. 按mid分组
        KeyedStream<JSONObject, String> keyDs = watermarkDs.keyBy(key -> key.getString("user_id"));

        //    TODO 6. 状态编程，取出每日加购独立用户数
        SingleOutputStreamOperator<CartAddUuBean> cartAddDs = keyDs.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            ValueState<String> lastCartAddState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> cartAdd = new ValueStateDescriptor<>("cartAdd", String.class);
                lastCartAddState = getRuntimeContext().getState(cartAdd);

                StateTtlConfig ttl = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                cartAdd.enableTimeToLive(ttl);

            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {
                String lastCartAddDate = lastCartAddState.value();
                String operate_time = jsonObject.getString("operate_time");
                String currentDate = null;

                if (operate_time != null) {
                    currentDate = operate_time.split(" ")[0];
                    ;
                } else {
                    String create_time = jsonObject.getString("create_time");
                    currentDate = create_time.split(" ")[0];
                    ;

                }


                if (lastCartAddDate == null || !currentDate.equals(lastCartAddDate)) {
                    lastCartAddState.update(currentDate);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }

            }
        });

        //    TODO 7. 开窗聚合

        SingleOutputStreamOperator<CartAddUuBean> resultDs = cartAddDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                        cartAddUuBean.setCartAddUuCt(cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt());
                        return cartAddUuBean;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean next = iterable.iterator().next();
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        String startDate = DateFormatUtil.toYmdHms(start);
                        String endDate = DateFormatUtil.toYmdHms(end);

                        collector.collect(new CartAddUuBean(startDate, endDate, next.getCartAddUuCt(), System.currentTimeMillis()));
                    }
                });

        //      TODO 8. 写出ck
        resultDs.addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //      TODO 9. 启动
        env.execute();


    }
}
