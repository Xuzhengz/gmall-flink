package com.xzz.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-12-21 10:14
 */
public class FlinkSqlJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));


        SingleOutputStreamOperator<JoinBean> source1 = env.socketTextStream("172.16.8.200", 9888).map(data -> {
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

        //将流转换成动态表
        tableEnv.createTemporaryView("s1", source1);
        tableEnv.createTemporaryView("s2", source2);

        /**
        inner join：A流：OnCreateAndWrite  B流：OnCreateAndWrite
        */
//        tableEnv.sqlQuery("select s1.id,s2.id,s1.name,s2.name,s1.ts,s2.ts from s1 join s2 on s1.id = s2.id").execute().print();

        /**
        left join：A流：OnReadAndWrite  B流：OnCreateAndWrite
        */
//        tableEnv.sqlQuery("select s1.id,s1.name,s1.ts,s2.id,s2.name,s2.ts from s1 left join s2 on s1.id = s2.id").execute().print();

        /**
        right join：A流：OnCreateAndWrite  B流：OnReadAndWrite
        */
//        tableEnv.sqlQuery("select s1.id,s1.name,s1.ts,s2.id,s2.name,s2.ts from s1 right join s2 on s1.id = s2.id").execute().print();


        /**
        right join：A流：OnReadAndWrite  B流：OnReadAndWrite
        */
        tableEnv.sqlQuery("select s1.id,s1.name,s1.ts,s2.id,s2.name,s2.ts from s1 full join s2 on s1.id = s2.id").execute().print();

    }
}
