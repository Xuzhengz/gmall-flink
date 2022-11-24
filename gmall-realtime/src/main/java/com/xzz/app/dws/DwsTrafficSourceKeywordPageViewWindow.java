package com.xzz.app.dws;

import com.xzz.app.function.SplitFunction;
import com.xzz.bean.KeywordBean;
import com.xzz.utils.KafkaUtil;
import com.xzz.utils.MyClickhouseUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.security.Key;

/**
 * @author 徐正洲
 * @create 2022-11-16 16:02
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //    TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        开启ck
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink/ck");
//        System.setProperty("HADOOP_USER_NAME","root");

        //    TODO 2. 使用Flink SQL 读取kafka_log数据创建表，并且提取时间戳生成watemark
        tableEnv.executeSql("" +
                "create table page_log(  " +
                "`page` map<string,string>, " +
                "`ts` bigint, " +
                "`rt` as to_timestamp(from_unixtime(ts/1000)), " +
                " watermark for rt as rt - interval '2' second " +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log", "Page_View_Window"));

        //    TODO 3. 过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("select" +
                " page['item'] item, " +
                " rt " +
                " from page_log " +
                " where page['item'] is not null " +
                " and page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' ");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //    TODO 4. 注册UDTF和切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table splitTable = tableEnv.sqlQuery("" +
                "select " +
                " word, " +
                " rt " +
                "from filter_table," +
                "lateral table(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table", splitTable);
        //    TODO 5. 分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select  " +
                " date_format(tumble_start(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(tumble_end(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss') edt, " +
                " 'search' source, " +
                " word keyword, " +
                " count(*) keyword_count, " +
                " unix_timestamp()*1000 ts " +
                " from " +
                " split_table  " +
                " group by word,tumble(rt,interval '10' second)");

        //    TODO 6. 将动态表转成流--字段名与bean的属性名字要一致，顺序无所谓。
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);

        keywordBeanDataStream.print();

        //    TODO 7. 将数据写入ck
        keywordBeanDataStream.addSink(MyClickhouseUtil.<KeywordBean>getClickhouseSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"
        ));

        //    TODO 8. 启动
        env.execute();

    }
}
