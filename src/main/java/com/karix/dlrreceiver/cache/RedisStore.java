package com.karix.dlrreceiver.cache;

import config.RedisBeanProvider;
import constants.RedisInternalDataConstants;
import org.springframework.stereotype.Component;

@Component
public class RedisStore {
    private final RedisBeanProvider redisBeanProvider;

    public RedisStore(RedisBeanProvider redisBeanProvider) {
        this.redisBeanProvider = redisBeanProvider;
    }

    public String getRequestPayload(String channel, String ackId) {
        return redisBeanProvider.getFeatureToRedisClientBean(
                        RedisInternalDataConstants.CONST_FEATURE_CD_GFU_FLAG).
                doGet(RedisInternalDataConstants.CONST_FEATURE_CD_GFU_FLAG + ":" + channel + ":" + ackId);
    }
}
