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

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> tClass, Boolean underScore) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        ArrayList<T> list = new ArrayList<>();


        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();


        //遍历结果集，转成T对象加入集合
        while (resultSet.next()) {
            //创建T对象
            T t = tClass.newInstance();


            //列遍历
            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(columnName);

                //判断是否需要下划线和驼峰属性进行转换
                if (underScore) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            preparedStatement.close();
            resultSet.close();

            //T对象加入集合
            list.add(t);

        }

        return list;
    }
}
