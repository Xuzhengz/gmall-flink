package com.xzz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 徐正洲
 * @date 2022/11/26-19:23
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;
}
