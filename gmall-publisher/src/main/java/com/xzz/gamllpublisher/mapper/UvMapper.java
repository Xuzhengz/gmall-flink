package com.xzz.gamllpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @date 2022/12/3-20:13
 */
public interface UvMapper {

    //查询ck 获取gmv总数
    @Select("select ch, sum(uv) uv, sum(uj_ct) uj from dws_traffic_channel_page_view_window where toYYYYMMDD(stt)=#{date} group by ch order by uv desc")
    List<Map> selectUvByCh(int date);
}