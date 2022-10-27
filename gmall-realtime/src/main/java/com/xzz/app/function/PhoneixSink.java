package com.xzz.app.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.DruidUtil;
import com.xzz.utils.PhoneixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

/**
 * @author 徐正洲
 * @create 2022-10-27 16:38
 * <p>
 * 自定义写入phoneix 维表数据
 */
public class PhoneixSink extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;

    /**
     * 构建连接，采用连接池。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidUtil.createDataSource();
    }

    /**
     * 写入phoneix
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        PhoneixUtil.upsertValues(connection, sinkTable, data);

        //关闭连接
        connection.close();
    }
}
