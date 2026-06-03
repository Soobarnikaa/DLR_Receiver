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
        System.out.println("=".repeat(50));
        System.out.println("Starting DLR Receiver Application...");
        System.out.println("Version: 1.1.0");
        System.out.println("Build Date: 2026-06-03");
        System.out.println("=".repeat(50));
        SpringApplication.run(DlrReceiverApplication.class, args);
    }
}
