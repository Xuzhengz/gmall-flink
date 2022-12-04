package com.xzz.gamllpublisher.service;

import com.xzz.gamllpublisher.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:34
 */
@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }
}