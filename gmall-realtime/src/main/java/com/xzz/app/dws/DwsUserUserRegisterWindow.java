package com.xzz.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.UserRegisterBean;
import com.xzz.utils.DateFormatUtil;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-11-25 16:48
 *
 * 用户域用户注册各窗口汇总表
 */
public class DwsUserUserRegisterWindow {
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
        String topic = "dwd_user_register";
        String groupId = "UUserRegisterWindow";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //    TODO 3. 转换json对象。
        SingleOutputStreamOperator<UserRegisterBean> beanDs = kafkaDs.map(map -> {
            JSONObject jsonObject = JSONObject.parseObject(map);
            String create_time = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(create_time, true));
        });


        //    TODO 4. 提取事件时间生成watermark

        SingleOutputStreamOperator<UserRegisterBean> watermarkDs = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                return userRegisterBean.getTs();
            }
        }));

        //    TODO 5. 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDs = watermarkDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                            @Override
                            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                                return value1;
                            }
                        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                                UserRegisterBean next = iterable.iterator().next();
                                long start = timeWindow.getStart();
                                long end = timeWindow.getEnd();
                                String startDate = DateFormatUtil.toYmdHms(start);
                                String endDate = DateFormatUtil.toYmdHms(end);

                                collector.collect(new UserRegisterBean(startDate, endDate, next.getRegisterCt(), System.currentTimeMillis()));

                            }
                        }
                );

        //    TODO 6. 写入ck
        resultDs.print("result：");
        resultDs.addSink(MyClickhouseUtil.getClickhouseSink("insert into dws_user_user_register_window values(?,?,?,?)"));
        //    TODO 7. 启动
        env.execute();

    }
}
