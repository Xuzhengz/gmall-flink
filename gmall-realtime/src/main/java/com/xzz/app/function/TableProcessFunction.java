package com.xzz.app.function;

import com.alibaba.fastjson.JSONObject;
import com.xzz.bean.TableProcess;
import com.xzz.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author 徐正洲
 * @date 2022/10/25-20:44
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 创建phoneix连接，一个并行度公用一个连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
//        1、获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(s);
//        转换成bean对象
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);

//        2、校验并建表
        checkTalbe(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

//        写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);
    }


    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//        1、获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(jsonObject.getString("table"));

        if (tableProcess != null) {
//        2、过滤字段
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
//        3、补充sinkTable，写出流
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        } else {
            System.out.println("找不到对应的表：" + jsonObject.getString("table"));
            return;
        }
    }

    /**
     过滤字段
    */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }


    /**
     * 校验并建表 create table if not exists db.tn(id varchar primary key id,bb varchar,cc varchar)
     */
    public void checkTalbe(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
//        处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuffer createTableSql = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");

//        判断是否主键
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key ");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                //        判断是否最后一个字段
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
//        拼接sql
            createTableSql.append(")").append(sinkExtend);
//        编译sql
            System.out.println("建表语句为： " + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());
//        执行sql
            preparedStatement.execute();

        } catch (SQLException throwables) {
//            编译型异常转运行型异常，停止任务，防止主流不断写入不存在的表
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}