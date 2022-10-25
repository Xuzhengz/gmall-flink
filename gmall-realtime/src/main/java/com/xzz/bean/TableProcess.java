package com.xzz.bean;

import lombok.Data;

/**
 * @author 徐正洲
 * @date 2022/10/25-20:38
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}