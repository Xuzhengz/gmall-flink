package com.xzz.gamllpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:27
 */

public interface GmvMapper {

    //查询ck 获取gmv总数
    @Select("select sum(order_amount) from dws_trade_user_spu_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);

}