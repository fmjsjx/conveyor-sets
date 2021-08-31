package com.github.fmjsjx.conveyor.core.input;

import java.util.function.Supplier;

import com.github.fmjsjx.conveyor.config.InputRedisConfig;
import com.github.fmjsjx.conveyor.util.ConfigUtil;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

public abstract class AbstractRedisInput implements Input {

    protected static final Supplier<StatefulRedisConnection<String, String>> toConnectionFactory(RedisClient client,
            String uri) {
        return () -> client.connect(RedisURI.create(uri));
    }

    protected final String name;
    protected final Supplier<StatefulRedisConnection<String, String>> redisConnectionFactory;
    protected final String key;
    protected volatile StatefulRedisConnection<String, String> redisConnection;

    protected AbstractRedisInput(String name, Supplier<StatefulRedisConnection<String, String>> redisConnectionFactory,
            String key) {
        this.name = name;
        this.redisConnectionFactory = redisConnectionFactory;
        this.key = key;
    }

    protected AbstractRedisInput(String name, RedisClient client, String productId, InputRedisConfig config) {
        this(name, toConnectionFactory(client, config.uri()), ConfigUtil.fixValue(config.key(), productId));
    }

    public StatefulRedisConnection<String, String> redisConnection() {
        var conn = redisConnection;
        if (conn == null) {
            synchronized (this) {
                conn = redisConnection;
                if (conn == null) {
                    redisConnection = conn = redisConnectionFactory.get();
                }
            }
        }
        return conn;
    }

    protected RedisCommands<String, String> redisSync() {
        return redisConnection().sync();
    }

    protected RedisAsyncCommands<String, String> redisAsync() {
        return redisConnection().async();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + name + ")";
    }

    @Override
    public synchronized void close() {
        var redisConnection = this.redisConnection;
        if (redisConnection != null) {
            redisConnection.close();
        }
    }

}
