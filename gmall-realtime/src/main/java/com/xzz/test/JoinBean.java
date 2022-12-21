package com.xzz.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 徐正洲
 * @create 2022-12-21 9:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JoinBean {
    int id;
    String name;
    Long ts;
}
