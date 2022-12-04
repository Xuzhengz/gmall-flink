package com.xzz.gamllpublisher.controller;

import com.xzz.gamllpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * @author 徐正洲
 * @date 2022/12/3-12:04
 */

//@Controller
@RequestMapping("/api/sugar")
@RestController // = @Controller +     @RequestBody
public class SugarController {

    @Autowired
    private GmvService gmvService;


    @RequestMapping("/test")
    public String test() {
        System.out.println("aaaaaaa");
        return "index.html";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam(value = "nn", defaultValue = "11") String name, @RequestParam(value = "age", defaultValue = "21") String age) {
        return name + age.toString();
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        BigDecimal bigDecimal = gmvService.getGmv(date);

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + bigDecimal +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        return Integer.parseInt(sdf.format(ts));

    }


}