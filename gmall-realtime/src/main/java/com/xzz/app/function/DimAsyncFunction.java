package com.xzz.app.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.xzz.utils.DimUtil;
import com.xzz.utils.DruidUtil;
import com.xzz.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 徐正洲
 * @create 2022-12-01 11:18
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private DruidDataSource dataSource = null;
    private ThreadPoolExecutor threadPoolExecutor = null;
    private String tableName;


    public DimAsyncFunction(String dim_sku_info) {
        this.tableName = dim_sku_info;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取连接
                    DruidPooledConnection connection = dataSource.getConnection();

                    // 查询维表信息
                    String key = getKey(t);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    //维度信息补充到当前数据
                    if (dimInfo != null) {
                        join(t, dimInfo);
                    }

                    //关闭连接
                    connection.close();

                    //写出
                    resultFuture.complete(Collections.singletonList(t));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败：" + t + ",Table：" + tableName);
                }

            }
        });
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
    }


}
