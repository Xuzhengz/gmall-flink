package com.xzz.gamllpublisher.controller;

import com.xzz.gamllpublisher.service.GmvService;
import com.xzz.gamllpublisher.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:04
 */

//@Controller
@RequestMapping("/api/sugar")
@RestController // = @Controller +     @RequestBody
public class SugarController {

    @Autowired
    @Qualifier("1")
    private GmvService gmvService;

    @Autowired
    private UvService uvService;


    @RequestMapping(value = "/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        Double bigDecimal = gmvService.getGmv(date);


        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + bigDecimal +
                "}";
    }

    @RequestMapping("/cs")
    public String getMes() {
        String message = gmvService.message();

        return message;
    }

    @RequestMapping("/ch")
    public String getUvByCh(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        Map uvByCh = uvService.getUvByCh(date);

        Set set = uvByCh.keySet();
        Collection values = uvByCh.values();

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\" " +
                StringUtils.join(set, "\",\"") +
                "    ], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"手机品牌\", " +
                "        \"data\": [\" " +
                StringUtils.join(values, "\",\"") +
                "        ] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }


    private int getToday() {
        long ts = System.currentTimeMillis();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        return Integer.parseInt(sdf.format(ts));

    }


}