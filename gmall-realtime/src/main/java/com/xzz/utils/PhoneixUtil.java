package com.xzz.utils;

import com.alibaba.fastjson.JSONObject;
import com.xzz.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author 徐正洲
 * @create 2022-10-27 16:51
 */
public class PhoneixUtil {

    public static void upsertValues(Connection connection, String sinkTable, JSONObject data) throws SQLException {
//        1、拼接SQL语句
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keys, ",") + ")" + " values(" +
                "'" + StringUtils.join(values, "','") + "')";
//        2、预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        3、执行
        preparedStatement.execute();
        connection.commit();
//        4、释放连接
        preparedStatement.close();
    }
}
