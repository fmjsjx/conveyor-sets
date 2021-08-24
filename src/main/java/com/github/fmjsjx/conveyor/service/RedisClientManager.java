package com.github.fmjsjx.conveyor.service;

import org.springframework.stereotype.Component;

import io.lettuce.core.RedisClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedisClientManager {

    private volatile RedisClient redisClient;

    public RedisClient globalRedisClient() {
        var redisClient = this.redisClient;
        if (redisClient == null) {
            synchronized (this) {
                redisClient = this.redisClient;
                if (redisClient == null) {
                    log.info("[redis-client:create] Create REDIS client");
                    this.redisClient = redisClient = RedisClient.create();
                }
            }
        }
        return redisClient;
    }

    public synchronized void shutdown() {
        var redisClient = this.redisClient;
        if (redisClient != null) {
            log.info("[app:shutdown] Shutdown REDIS client");
            redisClient.shutdown();
        }
    }

}
