package com.xzz.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 徐正洲
 * @create 2022-11-29 15:04
 * <p>
 * 适用于任何JDBC方式访问的数据库中的任何查询语句
 */
public class JdbcUtil {

    /**
     * Phoenix 表查询方法
     *
     * @param conn 数据库连接对象
     * @param sql  查询数据的 SQL 语句
     * @param clz  返回的集合元素类型的 class 对象
     * @param <T>  返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean underScore) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |

             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);

                    //判断是否需要下划线和驼峰属性进行转换
                    if (underScore) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }

                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

}
