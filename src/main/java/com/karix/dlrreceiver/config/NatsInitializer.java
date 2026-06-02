package com.karix.dlrreceiver.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.karix.nats.core.service.GenericInitializer;

import jakarta.annotation.PostConstruct;

@Component
public class NatsInitializer {

    private static final Logger log = LogManager.getLogger(NatsInitializer.class);

    @Value("${karix.nats.component}")
    private String component;

    @Value("${nats.config.path}")
    private String natsPath;

    @PostConstruct
    public void init() {
        log.info("Initializing NATS component={}", component);

        if (natsPath == null || natsPath.isBlank()) {
            throw new IllegalStateException(
                    "nats.config.path MUST be set and must point to directory containing nats.properties");
        }
        System.setProperty("nats.config.path", natsPath);
        log.info("Using NATS config directory: {}", natsPath);

        GenericInitializer.initialize(component, true);
    }
}

