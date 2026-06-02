package com.karix.dlrreceiver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"config","com.karix"})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
public class DlrReceiverApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlrReceiverApplication.class, args);
    }

}
