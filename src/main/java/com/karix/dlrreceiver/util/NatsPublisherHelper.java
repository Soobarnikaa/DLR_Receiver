package com.karix.dlrreceiver.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.karix.nats.core.producer.ProducerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NatsPublisherHelper {

    private static final Logger log = LogManager.getLogger(NatsPublisherHelper.class);

    private final ProducerManager producerManager;
    private final ObjectMapper mapper;

    public NatsPublisherHelper(ProducerManager producerManager, ObjectMapper mapper) {
        this.producerManager = producerManager;
        this.mapper = mapper;
    }

    public boolean publish(String subject, Object data, String ackId) throws JsonProcessingException {

        String message = mapper.writeValueAsString(data);

        boolean status = producerManager.publishAsyncMessage(subject, message, List.of(1));
        log.info("status:{}",status);

        if (!status) {
            log.error("NATS publish returned false for ackId={}", ackId);
        }

        return status;
    }
}