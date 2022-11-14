package com.xzz.app.dwd;

import com.xzz.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 徐正洲
 * @date 2022/11/14-20:32
 * 取消订单事实表
 */
public class DwdTradeCancelDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
        //TODO 2.消费kafka预处理表
        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "    `id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `create_time` string, " +
                "    `source_type_id` string, " +
                "    `source_type_name` string, " +
                "    `source_id` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `delivery_address` string, " +
                "    `order_comment` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `refundable_time` string, " +
                "    `order_detail_activity_id` string, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `order_detail_coupon_id` string, " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `type` string, " +
                "    `old` map<string,string> " +
                ") "
                + KafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "CancelOrderDetail"));

        //TODO 3.过滤取消订单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                "operate_time cancel_time, " +
                "source_id, " +
                "source_type source_type_id, " +
                "source_type_name, " +
                "sku_num, " +
                "order_price, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount " +
                "from dwd_order_pre " +
                "where `type` = 'update' " +
                "and `old`['order_status'] is not null " +
                "and order_status = '1003'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        //TODO 4.创建kakfa取消订单表
        tableEnv.executeSql("create table dwd_trade_cancel_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "cancel_time string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "order_price string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cancel_detail"));

        //TODO 5.写出
        tableEnv.sqlQuery("insert into dwd_trade_cancel_detail select * from filtered_table");
        
        //TODO 6.启动
        env.execute();

    }
}