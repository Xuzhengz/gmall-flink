package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.UserLoginBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author 徐正洲
 * @date 2022/11/24-20:25
 */
public class DwsUserUserLoginWindow {
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
        String groupId = "dws_userLogin_window";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //    TODO 3. 转换json对象，并过滤。
        SingleOutputStreamOperator<JSONObject> jsObj = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

                if (uid != null && (lastPage.equals("login") || lastPage == null)) {
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

        //    TODO 5. 按uid分组
        KeyedStream<JSONObject, String> keyDs = watermarkDs.keyBy(key -> key.getJSONObject("common").getString("uid"));

        //    TODO 6. 使用状态获取独立用户以及七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userLoginDs = keyDs.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLogin", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {
                String lastLoginDate = lastLoginState.value();
                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                Long uv = 0L;
                Long backUv = 0L;
                if (lastLoginDate == null) {
                    uv = 1L;
                    lastLoginState.update(currentDate);

                } else if (!lastLoginDate.equals(currentDate)) {
                    uv = 1L;
                    lastLoginState.update(currentDate);
                    if ((DateFormatUtil.toTs(currentDate) - (DateFormatUtil.toTs(lastLoginDate)) / (24 * 60 * 60 * 1000L)) >= 8L) {
                        backUv = 1L;
                    }
                }

                if (uv != 0L) {
                    collector.collect(new UserLoginBean("", "", backUv, uv, ts));
                }


            }
        });

        //    TODO 7. 开窗聚合

        SingleOutputStreamOperator<UserLoginBean> windowDs = userLoginDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                        userLoginBean.setBackCt(userLoginBean.getBackCt() + t1.getBackCt());
                        userLoginBean.setUuCt(userLoginBean.getUuCt() + t1.getUuCt());
                        return userLoginBean;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        UserLoginBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setTs(System.currentTimeMillis());

                        collector.collect(next);

                    }
                });


        //    TODO 8. 写入ck
        windowDs.print("result：");
        windowDs.addSink(MyClickhouseUtil.getClickhouseSink("" +
                "insert into dws_user_user_login_window values(?,?,?,?,?)"));

        //    TODO 9. 启动
        env.execute();
    }
}