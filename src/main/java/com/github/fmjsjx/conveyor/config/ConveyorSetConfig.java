package com.github.fmjsjx.conveyor.config;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.conveyor.util.CollectionUtil;
import com.github.fmjsjx.libcommon.util.NumberUtil;
import com.github.fmjsjx.libcommon.yaml.Jackson2YamlLibrary;

import lombok.ToString;

@ToString
public class ConveyorSetConfig {

    public static final ConveyorSetConfig loadFromYaml(InputStream in) {
        return Jackson2YamlLibrary.getInstance().loads(in, ConveyorSetConfig.class);
    }

    final String name;
    final boolean autoStart;
    final OptionalInt maxRetryCount;
    final List<String> products;
    final List<ConveyorConfig> conveyors;

    @JsonCreator
    public ConveyorSetConfig(@JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "auto-start", required = false) Boolean autoStart,
            @JsonProperty(value = "max-retry-count", required = false) Integer maxRetryCount,
            @JsonProperty(value = "products", required = true) List<String> products,
            @JsonProperty(value = "conveyors", required = true) List<ConveyorConfig> conveyors) {
        this.name = name;
        var nsp = name.split("\\s+");
        if (nsp.length > 1) {
            throw new IllegalArgumentException("there must not be any space in `name`");
        }
        this.autoStart = autoStart == null ? true : autoStart.booleanValue();
        this.maxRetryCount = NumberUtil.optionalInt(maxRetryCount);
        this.products = products.stream().peek(Objects::requireNonNull).map(String::strip).sorted()
                .collect(Collectors.toUnmodifiableList());
        this.conveyors = conveyors.stream().peek(Objects::requireNonNull).sorted()
                .collect(Collectors.toUnmodifiableList());
    }

    public String name() {
        return name;
    }

    public boolean autoStart() {
        return autoStart;
    }

    public OptionalInt maxRetryCount() {
        return maxRetryCount;
    }

    public List<String> products() {
        return products;
    }

    public List<ConveyorConfig> conveyors() {
        return conveyors;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConveyorSetConfig o) {
            if (name.equals(o.name) && autoStart == o.autoStart && maxRetryCount.equals(o.maxRetryCount)) {
                if (CollectionUtil.isEqual(products, o.products)) {
                    return CollectionUtil.isEqual(conveyors, o.conveyors);
                }
            }
        }
        return false;
    }

}
