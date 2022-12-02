package com.xzz.gamllpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:27
 */

public interface GmvMapper {

    //查询ck 获取gmv总数
    @Select("select sum(order_amount) from dwd_trade_province_order_window where toYYYYMMDD(stt)=#(date)")
    BigDecimal selectGmv(int date);

}