package com.github.fmjsjx.conveyor.config;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.conveyor.util.CollectionUtil;
import com.github.fmjsjx.libcommon.yaml.Jackson2YamlLibrary;

import lombok.ToString;

@ToString
public class ConveyorSetConfig {

    public static final ConveyorSetConfig loadFromYaml(InputStream in) {
        return Jackson2YamlLibrary.getInstance().loads(in, ConveyorSetConfig.class);
    }

    final String name;
    final List<Integer> products;
    final List<ConveyorConfig> conveyors;

    @JsonCreator
    public ConveyorSetConfig(@JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "products", required = true) List<Integer> products,
            @JsonProperty(value = "conveyors", required = true) List<ConveyorConfig> conveyors) {
        this.name = name;
        this.products = products.stream().peek(Objects::requireNonNull).sorted()
                .collect(Collectors.toUnmodifiableList());
        this.conveyors = conveyors.stream().peek(Objects::requireNonNull).sorted()
                .collect(Collectors.toUnmodifiableList());
    }

    public String name() {
        return name;
    }

    public List<Integer> products() {
        return products;
    }

    public List<ConveyorConfig> conveyors() {
        return conveyors;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConveyorSetConfig) {
            var o = (ConveyorSetConfig) obj;
            return name.equals(o.name) && CollectionUtil.isEqual(products, o.products)
                    && CollectionUtil.isEqual(conveyors, o.conveyors);
        }
        return false;
    }

}
