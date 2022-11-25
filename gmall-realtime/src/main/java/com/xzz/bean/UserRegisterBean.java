package com.xzz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 徐正洲
 * @create 2022-11-25 16:48
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
