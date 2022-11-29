package com.xzz.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TrafficPageViewBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/11/21-20:38
 *
 *
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
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
        //    TODO 2. 获取三个主题的数据创建流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String topic = "dwd_traffic_page_log";
        String groupId = "vccharisnew_pageview";
        DataStreamSource<String> uvStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId));
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        //    TODO 3. 统一数据格式
        SingleOutputStreamOperator<TrafficPageViewBean> uv = uvStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);

            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });
        SingleOutputStreamOperator<TrafficPageViewBean> uj = ujStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);

            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts")
            );
        });

        SingleOutputStreamOperator<TrafficPageViewBean> page = pageStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);

            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page1 = jsonObject.getJSONObject("page");
            JSONObject lastPageId = jsonObject.getJSONObject("last_page_id");

            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, sv, 1L, page1.getLong("during_time"), 0L,
                    jsonObject.getLong("ts")
            );
        });

        //    TODO 4. 三个流进行union
        DataStream<TrafficPageViewBean> unionDs = uv.union(uj, page);
        //    TODO 5. 提取事件时间生成watermark
        SingleOutputStreamOperator<TrafficPageViewBean> unionDsWithWaterMark = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        }));

        //    TODO 6. 分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = unionDsWithWaterMark.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4 getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return new Tuple4(trafficPageViewBean.getAr(), trafficPageViewBean.getCh(), trafficPageViewBean.getIsNew(), trafficPageViewBean.getVc());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> resultDs = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                TrafficPageViewBean next = iterable.iterator().next();

                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                next.setTs(System.currentTimeMillis());

                collector.collect(next);

            }
        });

        resultDs.print();
        //    TODO 7. 写出ck
        resultDs.addSink(MyClickhouseUtil.<TrafficPageViewBean>getClickhouseSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        //    TODO 8. 启动
        env.execute();


    }
}