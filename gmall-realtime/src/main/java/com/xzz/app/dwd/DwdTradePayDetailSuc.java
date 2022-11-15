package com.xzz.app.dwd;

import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 徐正洲
 * @create 2022-11-15 16:00
 *
 * dwd支付成功订单
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
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

        //TODO 2.读取topic_db数据过滤出支付成功数据
        tableEnv.executeSql(KafkaUtil.getTopicDb("PayDetailSuc"));

        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`pt` " +
                "from topic_db " +
                "where `table` = 'payment_info' " +
                "and `type` = 'update' " +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);


        //TODO 3.消费下单主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +
                "order_price string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " +
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail","PayDetailSuc"));
        

        //TODO 4.读取mysql-base_dic表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 5.进行三表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_id, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.order_price, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id = od.order_id " +
                "join `base_dic` for system_time as of pi.pt as dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);


        //TODO 6.构建支付成功表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "source_id string," +
                "source_type_id string," +
                "source_type_name string," +
                "sku_num string," +
                "order_price string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "primary key(order_detail_id) not enforced" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        //TODO 7.写出
        tableEnv.sqlQuery("insert into dwd_trade_pay_detail_suc select * from result_table");

        //TODO 8.启动
        env.execute();
    }
}
