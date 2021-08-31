package com.github.fmjsjx.conveyor.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.libcommon.util.NumberUtil;

import lombok.ToString;

@ToString
public class InputRedisConfig {

    public enum Type {
        LIST, STREAM
    }

    @ToString
    public static final class StreamConfig {

        final String group;
        final String consumer;

        @JsonCreator
        public StreamConfig(@JsonProperty(value = "group", required = true) String group,
                @JsonProperty(value = "consumer", required = true) String consumer) {
            this.group = group;
            this.consumer = consumer;
        }

        public String group() {
            return group;
        }

        public String consumer() {
            return consumer;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof StreamConfig) {
                var o = (StreamConfig) obj;
                return group.equals(o.group) && consumer.equals(o.consumer);
            }
            return false;
        }

    }

    final String uri;
    final String key;
    final int batch;
    final Type type;
    final StreamConfig stream;

    @JsonCreator
    public InputRedisConfig(@JsonProperty(value = "uri", required = true) String uri,
            @JsonProperty(value = "key", required = true) String key,
            @JsonProperty(value = "batch", required = false) Integer batch,
            @JsonProperty(value = "type", required = true) String type,
            @JsonProperty(value = "stream", required = false) StreamConfig stream) {
        this.uri = uri;
        this.key = key;
        this.batch = NumberUtil.intValue(batch, 200);
        this.type = Type.valueOf(type.toUpperCase());
        if (this.type == Type.STREAM) {
            this.stream = Objects.requireNonNull(stream, "`stream` must not be null when `type` is STREAM");
        } else {
            this.stream = null;
        }
    }

    public String uri() {
        return uri;
    }

    public String key() {
        return key;
    }

    public int batch() {
        return batch;
    }

    public Type type() {
        return type;
    }

    public StreamConfig stream() {
        return stream;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InputRedisConfig) {
            var o = (InputRedisConfig) obj;
            if (uri.equals(o.uri) && key.equals(o.key) && batch == o.batch && type.equals(o.type)) {
                if (type == Type.STREAM) {
                    return stream.equals(o.stream);
                }
                return true;
            }
        }
        return false;
    }

}
