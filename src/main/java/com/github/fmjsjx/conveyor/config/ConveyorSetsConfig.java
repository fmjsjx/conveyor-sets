package com.github.fmjsjx.conveyor.config;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.libcommon.yaml.Jackson2YamlLibrary;

import lombok.ToString;

@ToString
public class ConveyorSetsConfig {

    public static final ConveyorSetsConfig loadFromYaml(InputStream in) {
        return Jackson2YamlLibrary.getInstance().loads(in, ConveyorSetsConfig.class);
    }

    final Optional<UnixServerConfig> unixServer;
    final Optional<Resp3ServerConfig> resp3Server;
    final List<String> includes;

    @JsonCreator
    public ConveyorSetsConfig(@JsonProperty(value = "unix-server", required = false) UnixServerConfig unixServer,
            @JsonProperty(value = "resp3-server", required = false) Resp3ServerConfig resp3Server,
            @JsonProperty(value = "includes", required = true) List<String> includes) {
        this.unixServer = Optional.ofNullable(unixServer);
        this.resp3Server = Optional.ofNullable(resp3Server);
        this.includes = includes.stream().map(String::strip).distinct().toList();
    }

    public Optional<UnixServerConfig> unixServer() {
        return unixServer;
    }

    public Optional<Resp3ServerConfig> resp3Server() {
        return resp3Server;
    }

    public List<String> includes() {
        return includes;
    }

}
