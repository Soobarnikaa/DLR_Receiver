package com.karix.dlrreceiver.service;

import com.karix.commonutil.enums.Category;
import com.karix.commonutil.enums.Channel;
import config.RedisBeanProvider;
import constants.RedisInternalDataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Service
public class RedisService {

    Logger log = LoggerFactory.getLogger(getClass());

    private final RedisBeanProvider redisBeanProvider;

    @Value("${aggregation.sub.redis_status_expiry_hours.sms:12}")
    private int aggregationSubRedisStatusExpiryHoursSMS;

    @Value("${aggregation.sub.redis_status_expiry_hours.email:48}")
    private int aggregationSubRedisStatusExpiryHoursEmail;

    private static final String FEATURE = RedisInternalDataConstants.CONST_FEATURE_DLR_RECEIVER;
    private static final String SUB_UPLOAD_PAYLOAD = "SUB_UPLOAD_PAYLOAD";
    private static final String DN_UPLOAD_PAYLOAD = "DN_UPLOAD_PAYLOAD";
    private static final String UPLOAD_TTL = "UPLOAD_TTL";

    public RedisService(RedisBeanProvider redisBeanProvider) {
        this.redisBeanProvider = redisBeanProvider;
    }

    private String buildKey(String app, Channel channel, String ackId, String mid, String action) {
        StringBuilder sb = new StringBuilder(app)
                .append(":").append(channel)
                .append(":").append(action).append(":")
                .append(ackId).append(":")
                .append(mid);
        return sb.toString();
    }

    private String buildKey(Channel channel, String ackId, String mid, String action) {
        return buildKey(FEATURE, channel, ackId, mid, action);
    }

    public long getSubExpiryHours(Channel channel) {
        if (Channel.SMS.equals(channel)) {
            return aggregationSubRedisStatusExpiryHoursSMS;
        } else if (Channel.EMAIL.equals(channel)) {
            return aggregationSubRedisStatusExpiryHoursEmail;
        } else {
            return 24;
        }
    }

    public String getSubUploadPayload(Channel channel, String ackId, String mid) {
        String key = buildKey(channel, ackId, mid, SUB_UPLOAD_PAYLOAD);
        String val = redisBeanProvider
                .getFeatureToRedisClientBean(FEATURE)
                .doGet(key);
        log.debug("getSubUploadPayload : key={}, value = {}", key, val);
        return val;
    }

    public String getDnUploadPayload(Channel channel, String ackId, String mid) {
        String key = buildKey(channel, ackId, mid, DN_UPLOAD_PAYLOAD);
        String val = redisBeanProvider
                .getFeatureToRedisClientBean(FEATURE)
                .doGet(key);
        log.debug("getDnUploadPayload : key={}, value = {}", key, val);
        return val;
    }

    public void setSubUploadPayload(Channel channel, Category category, String ackId, String mid, String value, String clientId) {
        String key = buildKey(channel, ackId, mid, SUB_UPLOAD_PAYLOAD);
        log.debug("setSubUploadPayload : key={}, value={}", key, value);
        redisBeanProvider
                .getFeatureToRedisClientBean(FEATURE)
                .doSet(key, value);

        // Add it to the sorted set too
        this.addTTLForPayload(channel, category, ackId, mid, "SUB", clientId);
    }

    public void setDnUploadPayload(Channel channel, Category category, String ackId, String mid, String value, String clientId) {
        String key = buildKey(channel, ackId, mid, DN_UPLOAD_PAYLOAD);
        log.debug("setDnUploadPayload : key={}, value={}", key, value);
        redisBeanProvider
                .getFeatureToRedisClientBean(FEATURE)
                .doSet(key, value);

        // Add it to the sorted set too
        this.addTTLForPayload(channel, category, ackId, mid, "DN",clientId);
    }

    //score is the time at which the payload will expire.
    private void addTTLForPayload(Channel channel, Category category, String ackId, String mid, String type, String clientId) {
        String val = channel + "|" + category + "|" + ackId + "|" + mid + "|" + type + "|" + clientId;
        log.debug("Adding TTL for payload : {}", val);
        long now = Instant.now().getEpochSecond(); // in seconds
        long score = now + (getSubExpiryHours(channel) * 60 * 60);
        boolean setAddStatus = redisBeanProvider.getFeatureToRedisClientBean(FEATURE)
                .doZAddSortedSet(UPLOAD_TTL, score, val);
        log.debug("addTTLForSubPayload : key = {}, val = {}, score = {}, status = {}",
                UPLOAD_TTL, val, score, setAddStatus);
    }

    public void deleteSubUploadPayload(Channel channel, Category category, String ackId, String mid, String clientId) {
        String key = buildKey(channel, ackId, mid, SUB_UPLOAD_PAYLOAD);
        log.debug("deleteSubUploadPayload : key={}", key);
        redisBeanProvider.getFeatureToRedisClientBean(FEATURE)
                .doDelWithKey(key);

        // Remove from sorted set too
        this.removeTTLForPayload(channel, category, ackId, mid, "SUB",clientId);
    }

    public void deleteDnUploadPayload(Channel channel, Category category, String ackId, String mid, String clientId) {
        String key = buildKey(channel, ackId, mid, DN_UPLOAD_PAYLOAD);
        log.debug("deleteDnUploadPayload : key={}", key);
        redisBeanProvider.getFeatureToRedisClientBean(FEATURE)
                .doDelWithKey(key);

        // Remove from sorted set too
        this.removeTTLForPayload(channel, category, ackId, mid, "DN", clientId);
    }

    public void removeTTLForPayload(Channel channel, Category category, String ackId, String mid, String type, String clientId) {
        String val = channel + "|" + category + "|" + ackId + "|" + mid + "|" + type + "|" + clientId;
        log.debug("Removing TTL for payload : {}", val);
        this.removeTTLForPayload(val);
    }

    public void removeTTLForPayload(String val) {
        Long setRes = redisBeanProvider.getFeatureToRedisClientBean(FEATURE)
                .doZSetRemove(UPLOAD_TTL, val);
        log.debug("removeTTLForSubPayload : key = {}, val = {}, status = {}",
                UPLOAD_TTL, val, setRes);
    }


    public Set<String> fetchExpiredUploadPayloads() {
        long now = Instant.now().getEpochSecond(); // in seconds
        Set<Object> raw = redisBeanProvider.getFeatureToRedisClientBean(FEATURE)
                .doZRangeSortedSet(UPLOAD_TTL, 0, now);

        Set<String> result = new HashSet<>();
        if (raw != null) {
            for (Object o : raw) {
                result.add(String.valueOf(o));
            }
        }
        log.debug("fetchExpiredSubUploadPayloads : key = {}, expiry = {}, resultSize = {}, result = {}", UPLOAD_TTL,
                now, result.size(), result);
        return result;
    }

}
