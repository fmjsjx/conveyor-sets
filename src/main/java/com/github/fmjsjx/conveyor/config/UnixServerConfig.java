package com.github.fmjsjx.conveyor.config;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.ToString;

@ToString
public class UnixServerConfig {

    final String file;
    final Optional<String> username;
    final Optional<String> password;

    @JsonCreator
    public UnixServerConfig(@JsonProperty(value = "file", required = true) String file,
            @JsonProperty(value = "username", required = false) String username,
            @JsonProperty(value = "password", required = false) String password) {
        this.file = file;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
    }

    public String file() {
        return file;
    }

    public Optional<String> username() {
        return username;
    }

    public Optional<String> password() {
        return password;
    }

}
