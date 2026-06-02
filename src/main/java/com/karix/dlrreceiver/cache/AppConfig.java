package com.karix.dlrreceiver.cache;

import com.karix.dlrreceiver.exception.CriticalInitializationException;
import config.RedisBeanProvider;
import constants.RedisInternalDataConstants;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.redis.spring.RedisLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class AppConfig {

    @Bean
    LockProvider lockProvider(@Autowired RedisBeanProvider redisBeanProvider) {
        final String ENV = "dlrreceiver";
        RedisTemplate<Object, Object> redisTemplate = redisBeanProvider
                .getFeatureToRedisClientBean(RedisInternalDataConstants.CONST_FEATURE_DLR_RECEIVER)
                .getRedisTemplate();

        RedisConnectionFactory redisConnectionFactory = redisTemplate.getConnectionFactory();

        if (redisConnectionFactory != null) {
            return new RedisLockProvider(redisConnectionFactory, ENV);
        } else {
            throw new CriticalInitializationException(
                    "Redis connection factory is unavailable which is required for shedlock to run.");
        }
    }

}
