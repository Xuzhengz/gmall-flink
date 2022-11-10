package com.xzz.app.dwd;

import com.xzz.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 徐正洲
 * @create 2022-11-10 16:50
 *
 * 交易域加购事实表
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
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

        //TODO 2.读取topic_db数据 使用DDL方式创建表
        tableEnv.executeSql(KafkaUtil.getTopicDb("TradeCartAdd"));

        //TODO 3.过滤出加购数据
        Table cartAdd = tableEnv.sqlQuery("" +
                "SELECT  " +
                "  data['id']  id,  " +
                "  data['user_id']  user_id,  " +
                "  data['sku_id']  sku_id,  " +
                "  data['cart_price']  cart_price,  " +
                "  if(`type` = 'insert',data['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,  " +
                "  data['img_url']  img_url,  " +
                "  data['sku_name']  sku_name,  " +
                "  data['is_checked']  is_checked,  " +
                "  data['create_time']  create_time,  " +
                "  data['operate_time']  operate_time,  " +
                "  data['is_ordered']  is_ordered,  " +
                "  data['source_type']  source_type,  " +
                "  data['source_id']  source_id,  " +
                "  pt   " +
                "FROM  " +
                "  topic_db   " +
                "WHERE  " +
                "  `DATABASE` = 'gmall-flink'   " +
                "  and `table` = 'cart_info'   " +
                "  and `type` = 'insert'   " +
                "  or (`type` = 'update'   " +
                "  and `old`['sku_name'] is not null   " +
                "  and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))");

        //TODO 4.读取mysql的 bash_dic表作为lookup表
        //TODO 5.关联表
        //TODO 6.Flink SQL 创建加购事实表
        //TODO 7.写出到kafka
        //TODO 8.执行




    }
}
