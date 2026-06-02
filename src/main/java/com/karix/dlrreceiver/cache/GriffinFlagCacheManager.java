package com.karix.dlrreceiver.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.karix.commonutil.enums.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Component
public class GriffinFlagCacheManager {

    private static final Logger log = LogManager.getLogger(GriffinFlagCacheManager.class);

    private final LoadingCache<String, Optional<String>> cache;
    private final RedisStore redisStore;

    public GriffinFlagCacheManager(
            RedisStore redisStore,
            @Value("${griffin.cache.ttl.minutes:2880}") long ttlMinutes,
            @Value("${griffin.cache.max.size:500000}") long maxSize) {

        this.redisStore = redisStore;

        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .maximumSize(maxSize)
                .recordStats()
                .build(CacheLoader.from(this::load));

        log.debug("GriffinFlagCacheManager initialized ttl={}min maxSize={}", ttlMinutes, maxSize);
    }

    public Optional<String> get(Channel channel, String ackId) {
        if (channel == null || ackId == null || ackId.isBlank()) {
            return Optional.empty();
        }
        try {
            return cache.get(cacheKey(channel, ackId));
        } catch (ExecutionException e) {
            log.error("Cache load failed channel={} ackId={}, treating as empty",
                    channel, ackId, e.getCause());
            return Optional.empty();
        }
    }

    public void put(Channel channel, String ackId, String flag) {
        if (channel == null || ackId == null || ackId.isBlank()) return;
        cache.put(cacheKey(channel, ackId), Optional.ofNullable(flag));
        log.debug("Cache primed channel={} ackId={} griffinUploadRequired={}", channel, ackId, flag);
    }

    private Optional<String> load(String key) {
        int sep = key.indexOf(':');
        if (sep < 0) {
            log.warn("Malformed cache key='{}', cannot load", key);
            return Optional.empty();
        }

        String channelStr = key.substring(0, sep);
        String ackId      = key.substring(sep + 1);

        try {
            Channel channel = Channel.valueOf(channelStr);
            return switch (channel) {
                case SMS   -> loadSms(ackId);
                case EMAIL -> loadEmail(ackId);
            };
        } catch (IllegalArgumentException e) {
            log.warn("Unknown channel='{}' in cache key, ackId={}", channelStr, ackId);
            return Optional.empty();
        }
    }

    private Optional<String> loadSms(String ackId) {
        try {
            String flag = redisStore.getRequestPayload(Channel.SMS.name(), ackId);
            log.debug("SMS Griifin upload flag loaded from redis ackId={} flag={}", ackId, flag);
            if (flag == null) {
                log.debug("SMS Redis miss ackId={}", ackId);
                return Optional.empty();
            }

            if (flag.isBlank()) {
                log.debug("SMS Redis miss ackId={}", ackId);
                return Optional.empty();
            }

            log.debug("SMS Redis loaded ackId={} griffinUploadRequired={}", ackId, flag);
            return Optional.of(flag);

        } catch (Exception e) {
            log.error("Failed to load SMS griffin flag ackId={}", ackId, e);
            return Optional.empty();
        }
    }

    private Optional<String> loadEmail(String ackId) {
        try {
            String flag = redisStore.getRequestPayload(Channel.EMAIL.name(), ackId);
            log.debug("EMAIL Griifin upload flag loaded from redis ackId={} flag={}", ackId, flag);
            if (flag == null) {
                log.debug("EMAIL Redis miss ackId={}", ackId);
                return Optional.empty();
            }

            if (flag.isBlank()) {
                log.debug("EMAIL Redis miss ackId={}", ackId);
                return Optional.empty();
            }

            log.debug("EMAIL Redis loaded ackId={} griffinUploadRequired={}", ackId, flag);
            return Optional.of(flag);

        } catch (Exception e) {
            log.error("Failed to load EMAIL griffin flag ackId={}", ackId, e);
            return Optional.empty();
        }
    }

    private static String cacheKey(Channel channel, String ackId) {
        return channel.name() + ":" + ackId;
    }
}