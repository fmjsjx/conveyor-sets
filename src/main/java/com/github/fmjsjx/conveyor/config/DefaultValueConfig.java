package com.github.fmjsjx.conveyor.config;

import com.github.fmjsjx.libcommon.util.StringUtil;

public class DefaultValueConfig {

    final String content;

    public DefaultValueConfig(String content) {
        this.content = content;
    }

    public boolean isPresent() {
        return content != null;
    }

    public String content() {
        return content;
    }

    @Override
    public String toString() {
        return content();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultValueConfig) {
            return obj == this || StringUtil.isEquals(content, ((DefaultValueConfig) obj).content);
        }
        return false;
    }

}
