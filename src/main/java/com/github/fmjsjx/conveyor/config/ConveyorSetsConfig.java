package com.github.fmjsjx.conveyor.config;

import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.libcommon.yaml.Jackson2YamlLibrary;

import lombok.ToString;

@ToString
public class ConveyorSetsConfig {

    public static final ConveyorSetsConfig loadFromYaml(InputStream in) {
        return Jackson2YamlLibrary.getInstance().loads(in, ConveyorSetsConfig.class);
    }

    final List<String> includes;

    @JsonCreator
    public ConveyorSetsConfig(@JsonProperty(value = "includes", required = true) List<String> includes) {
        this.includes = includes.stream().map(String::strip).toList();
    }

    public List<String> includes() {
        return includes;
    }

}
