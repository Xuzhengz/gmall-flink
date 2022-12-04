package com.xzz.gamllpublisher.service;

import com.xzz.gamllpublisher.mapper.UvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @date 2022/12/3-20:22
 */
@Service
public class UvServiceImpl implements UvService {

    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map getUvByCh(int date) {
        //创建hashmap
        HashMap<String, BigInteger> resultMap = new HashMap<>();

        List<Map> maps = uvMapper.selectUvByCh(date);

        for (Map map : maps) {
            resultMap.put((String) map.get("ch"), (BigInteger) map.get("uv"));
        }

        return resultMap;
    }
}