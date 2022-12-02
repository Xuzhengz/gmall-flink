package com.xzz.gamllpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.xzz.gamllpublisher.mapper")
public class GamllPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GamllPublisherApplication.class, args);
    }

}
