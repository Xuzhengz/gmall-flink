package com.xzz.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.beans.Transient;

/**
 * @author 徐正洲
 * @date 2022/11/17-20:37
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeywordBean {

    private String stt;
    private String edt;

    @TransientSink
    private String source;
    private String keyword;
    private Long keyword_count;
    private Long ts;

}