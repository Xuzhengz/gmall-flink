package com.xzz.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-12-21 9:57
 */
public class IntervalJoinTest {
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

        SingleOutputStreamOperator<Tuple2<JoinBean, JoinBean>> result = source1.keyBy(JoinBean::getId).intervalJoin(source2.keyBy(JoinBean::getId)).between(Time.seconds(-5), Time.seconds(5)).process(new ProcessJoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>() {
            @Override
            public void processElement(JoinBean joinBean, JoinBean joinBean2, ProcessJoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>.Context context, Collector<Tuple2<JoinBean, JoinBean>> collector) throws Exception {
                collector.collect(new Tuple2<>(joinBean, joinBean2));
            }
        });

        result.print("result>>>>>");

        env.execute();
    }
}
