package com.xzz.utils;

import com.alibaba.fastjson.JSONObject;
import com.xzz.common.GmallConfig;
import org.apache.phoenix.util.JDBCUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author 徐正洲
 * @create 2022-11-29 15:55
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        String sql = "" +
                "select * from" +
                GmallConfig.HBASE_SCHEMA +
                "." + tableName +
                " where id = '" + key + "'";
        System.out.println(sql);

        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, false);

        return jsonObjects.get(0);

    }


}
