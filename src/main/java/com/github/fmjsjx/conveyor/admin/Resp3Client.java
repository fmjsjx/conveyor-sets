package com.github.fmjsjx.conveyor.admin;

import com.github.fmjsjx.conveyor.config.Resp3ServerConfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

public class Resp3Client {

    private Resp3ServerConfig config;

    public Resp3Client(Resp3ServerConfig config) {
        this.config = config;
    }

    public void command(String[] args) throws Exception {
        var uri = RedisURI.builder().withHost(config.address().orElse("127.0.0.1")).withPort(config.port());
        config.password().ifPresent(password -> {
            config.username().ifPresentOrElse(username -> uri.withAuthentication(username, password),
                    () -> uri.withPassword((CharSequence) password));
        });
        var client = RedisClient.create(uri.build());
        try (var conn = client.connect()) {
            var key = String.join(" ", args);
            var value = conn.sync().get(key);
            System.out.println(value);
        } finally {
            client.shutdown();
        }
    }

}
