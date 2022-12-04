package com.xzz.gamllpublisher.service;

import com.xzz.gamllpublisher.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:34
 */
@Service("2")
public class GmvServiceImpl2 implements GmvService {

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }

    @Override
    public String message() {
        return "2";
    }
}