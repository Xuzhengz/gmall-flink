package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TrafficHomeDetailPageViewBean;
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
 * @date 2022/11/23-20:51
 * <p>
 * 流量域页面浏览各窗口汇总表
 */
public class DwsTrafficPageViewWindow {
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
        SingleOutputStreamOperator<JSONObject> watermarkDs = jsObj.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        //    TODO 5. 按mid分组
        KeyedStream<JSONObject, String> keyDs = watermarkDs.keyBy(key -> key.getJSONObject("common").getString("mid"));

        //    TODO 6. 使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> flatMapDs = keyDs.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeState;
            ValueState<String> detailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> home = new ValueStateDescriptor<>("home", String.class);
                ValueStateDescriptor<String> detail = new ValueStateDescriptor<>("detail", String.class);
                homeState = getRuntimeContext().getState(home);
                detailState = getRuntimeContext().getState(detail);

                StateTtlConfig ttl = StateTtlConfig.newBuilder(
                        Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                home.enableTimeToLive(ttl);
                detail.enableTimeToLive(ttl);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取状态数据和数据中的日期
                String homeLastDate = homeState.value();
                String detailLastDate = detailState.value();

                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                //定义访问首页或详情数据

                Long homeCt = 0L;
                Long detailCt = 0L;
                if ("home".equals(jsonObject.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDate == null || !homeLastDate.equals(currentDate)) {
                        homeCt = 1L;
                        homeState.update(currentDate);
                    }
                }else {
                    if (detailLastDate == null || !detailLastDate.equals(currentDate)) {
                        detailCt = 1L;
                        detailState.update(currentDate);
                    }
                }



                //满足任何一个数据不为0 则写出
                if (homeCt == 1L || detailCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, detailCt, ts));
                }
            }
        });


        //    TODO 7. 开窗聚合

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowDs = flatMapDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                return value1;
                            }
                        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(timeWindow.getStart());
                                String edt = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                                TrafficHomeDetailPageViewBean next = iterable.iterator().next();

                                next.setStt(stt);
                                next.setEdt(edt);
                                next.setTs(System.currentTimeMillis());

                                collector.collect(next);
                            }
                        }
                );


        //    TODO 8. 写出ck
        windowDs.print("result：");
        windowDs.addSink(MyClickhouseUtil.getClickhouseSink("" +
                "insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        //    TODO 9. 启动
        env.execute();
    }
}