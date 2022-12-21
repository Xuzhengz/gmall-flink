package com.xzz.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-12-21 9:20
 */
public class WindowJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<JoinBean> source1 = env.socketTextStream("172.16.8.200", 8888).map(data -> {
            String[] fields = data.split(",");
            return new JoinBean(new Integer(fields[0]), fields[1], new Long(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JoinBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JoinBean>() {
            @Override
            public long extractTimestamp(JoinBean joinBean, long l) {
                return joinBean.getTs() * 1000L;
            }
        }));

        SingleOutputStreamOperator<JoinBean> source2 = env.socketTextStream("172.16.8.200", 9999).map(data -> {
            String[] fields = data.split(",");
            return new JoinBean(new Integer(fields[0]), fields[1], new Long(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JoinBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JoinBean>() {
            @Override
            public long extractTimestamp(JoinBean joinBean, long l) {
                return joinBean.getTs() * 1000L;
            }
        }));

        DataStream<Tuple2<JoinBean, JoinBean>> result = source1.join(source2).where(JoinBean::getId).equalTo(JoinBean::getId).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new JoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>() {
            @Override
            public Tuple2<JoinBean, JoinBean> join(JoinBean joinBean, JoinBean joinBean2) throws Exception {
                return new Tuple2<>(joinBean, joinBean2);
            }
        });

        result.print("result>>>>>>>>>>>");

        env.execute();


    }
}
