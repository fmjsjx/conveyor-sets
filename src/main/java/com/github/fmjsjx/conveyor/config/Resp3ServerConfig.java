package com.github.fmjsjx.conveyor.config;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.ToString;

@ToString
public class Resp3ServerConfig {

    final Optional<String> address;
    final int port;
    final Optional<String> username;
    final Optional<String> password;

    @JsonCreator
    public Resp3ServerConfig(@JsonProperty(value = "address", required = false) String address,
            @JsonProperty(value = "port", required = true) int port,
            @JsonProperty(value = "username", required = false) String username,
            @JsonProperty(value = "password", required = false) String password) {
        this.address = Optional.ofNullable(address);
        this.port = port;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
    }

    public Optional<String> address() {
        return address;
    }

    public int port() {
        return port;
    }

    public Optional<String> username() {
        return username;
    }

    public Optional<String> password() {
        return password;
    }

}
