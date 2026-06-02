package com.karix.dlrreceiver.service;

import com.karix.dlrreceiver.exception.QueueException;
import config.RedisBeanProvider;
import constants.RedisInternalDataConstants;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import utils.RedisClient;

@Service
public class RedisQueueService {

    private static final Logger log = LogManager.getLogger(RedisQueueService.class);
    private static final String FEATURE = RedisInternalDataConstants.CONST_FEATURE_DLR_RECEIVER;

    private final RedisBeanProvider redisBeanProvider;

    @Value("${redis.queue.name}")
    private String queue;

    private RedisClient client;

    public RedisQueueService(RedisBeanProvider redisBeanProvider) {
        this.redisBeanProvider = redisBeanProvider;
    }

    @PostConstruct
    public void init() {
        this.client = redisBeanProvider.getFeatureToRedisClientBean(FEATURE);

        if (this.client == null) {
            throw new IllegalStateException(
                    "RedisClient is null for feature=" + FEATURE +
                            ". Check redis.bean.create.enabled and component.features config."
            );
        }

        log.debug("RedisClient initialized successfully for feature={}", FEATURE);
    }

    public Object push(String payload) {
        try {

            Object result = client.doLPush(queue, payload);

            log.debug("[LPUSH] queue={} result={} payload={}", queue, result, payload);

            return result;

        } catch (Exception e) {
            log.error("[LPUSH_ERROR] queue={} error={}", queue, e.getMessage(), e);
            throw new QueueException("LPUSH failed", e);
        }
    }

    public String take() {
        try {
            Object val = client.doRPOP(queue);
            String result = val != null ? val.toString() : null;

            if (result != null) {
                log.debug("[POP]  queue={} payload={}", queue, result);
            } else {
                log.debug("[POP]  queue={} no message (timeout)", queue);
            }

            return result;

        } catch (Exception e) {
            log.error("[RPOP_ERROR] queue={} error={}", queue, e.getMessage(), e);
            throw new QueueException("RPOP failed", e);
        }
    }
}