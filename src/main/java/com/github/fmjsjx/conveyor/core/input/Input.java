package com.github.fmjsjx.conveyor.core.input;

import java.util.List;
import java.util.Map;

public interface Input extends AutoCloseable {

    List<Map<String, String>> fetch();

    @Override
    void close();

}
