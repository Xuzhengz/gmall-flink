package com.xzz.utils;

import com.alibaba.fastjson.JSONObject;
import com.xzz.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author 徐正洲
 * @create 2022-11-29 15:55
 */
public class DimUtil {

    public static void delDimInfo(String tableName, String key) {
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;

        jedis.del(redisKey);

        jedis.close();
    }

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {

        //查询缓存,若查询为空，则从phoneix查询
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);

        if (dimJsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //关闭时间
            jedis.close();
            //返回维度数据
            return JSONObject.parseObject(dimJsonStr);
        }


        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + key + "'";
        System.out.println(sql);

        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, false);

        JSONObject jsonObject = jsonObjects.get(0);

        //phoneix查询的数据写入redis

        jedis.set(redisKey, jsonObject.toJSONString());

        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        //关闭时间
        jedis.close();

        return jsonObject;

    }


}
