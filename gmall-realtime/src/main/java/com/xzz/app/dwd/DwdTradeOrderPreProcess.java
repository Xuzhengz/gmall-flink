package com.xzz.app.dwd;

import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/11/13-11:38
 * <p>
 * 交易域预处理表
 */
public class DwdTradeOrderPreProcess {
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

        //设置状态的TTL 生产环境设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //TODO 2.创建topic_db表
        tableEnv.executeSql(KafkaUtil.getTopicDb("Dwd_Trade_Order_Pre_Process"));

        //TODO 3.过滤订单明细数据
        Table orderTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt  " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail'  " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("orderTable", orderTable);
        tableEnv.toAppendStream(orderTable, Row.class).print();

        //TODO 4.过滤订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['delivery_address'] delivery_address, " +
                "    data['order_comment'] order_comment, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    data['refundable_time'] refundable_time, " +
                "    `type`, " +
                "    `old` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info'  " +
                "and (`type` = 'insert' or `type` = 'update')");

        tableEnv.createTemporaryView("orderInfoTable", orderInfoTable);
        tableEnv.toAppendStream(orderInfoTable, Row.class).print();

        //TODO 5.过滤出订单明细活动关联数据
        Table orderDetailActivity = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['activity_id'] activity_id, " +
                "    data['activity_rule_id'] activity_rule_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity'  " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("orderDetailActivity", orderDetailActivity);
        tableEnv.toAppendStream(orderDetailActivity, Row.class).print();

        //TODO 6.过滤出订单明细加购物卷关联数据
        Table orderDetailCoupon = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'], " +
                "    data['order_id'], " +
                "    data['order_detail_id'], " +
                "    data['coupon_id'], " +
                "    data['coupon_use_id'], " +
                "    data['sku_id'], " +
                "    data['create_time'] " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon'  " +
                "and `type` = 'insert'");

        tableEnv.createTemporaryView("orderDetailCoupon", orderDetailCoupon);
        tableEnv.toAppendStream(orderDetailCoupon, Row.class).print();

        //TODO 7.创建 base_dic lookup表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 8.关联5张表
        Table dwdOrderPreTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.id order_detail_activity_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.id order_detail_coupon_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    oi.`type`, " +
                "    oi.`old`, " +
                "    current_row_timestamp() row_op_ts, " +
                "from order_detail_table od " +
                "join order_info_table oi " +
                "on od.order_id = oi.id " +
                "left join order_activity_table oa " +
                "on od.id = oa.order_detail_id " +
                "left join order_coupon_table oc " +
                "on od.id = oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("dwdOrderPreTable", dwdOrderPreTable);

        //TODO 9.创建 upsert kafka连接器
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
                "    `row_op_ts` timestamp_ltz(3) " +
                ") "
        +KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));


        //TODO 10.将数据写出
        tableEnv.sqlQuery("insert into dwd_order_pre select * from dwdOrderPreTable");

        //TODO 11.启动
        env.execute();
    }
}