package com.github.fmjsjx.conveyor.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.libcommon.util.StringUtil;

import lombok.ToString;

@ToString
public class ConveyorConfig implements Comparable<ConveyorConfig> {

    final String name;
    final InputRedisConfig inputRedis;
    final OutputMysqlConfig outputMysql;

    @JsonCreator
    public ConveyorConfig(@JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "input.redis", required = true) InputRedisConfig inputRedis,
            @JsonProperty(value = "output.mysql", required = true) OutputMysqlConfig outputMysql) {
        this.name = name;
        this.inputRedis = inputRedis;
        this.outputMysql = outputMysql;
    }

    public String name() {
        return name;
    }

    public InputRedisConfig inputRedis() {
        return inputRedis;
    }

    public OutputMysqlConfig outputMysql() {
        return outputMysql;
    }

    @Override
    public int compareTo(ConveyorConfig o) {
        return name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConveyorConfig) {
            var o = (ConveyorConfig) obj;
            if (StringUtil.isEquals(name, o.name)) {
                if (inputRedis.equals(o.inputRedis)) {
                    return outputMysql.equals(o.outputMysql);
                }
            }
        }
        return false;
    }

}
