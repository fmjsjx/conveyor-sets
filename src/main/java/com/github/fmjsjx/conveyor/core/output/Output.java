package com.github.fmjsjx.conveyor.core.output;

import java.util.List;
import java.util.Map;

public interface Output extends AutoCloseable {

    void push(List<Map<String, String>> batch) throws Exception;
    
    void failed(List<Map<String, String>> batch);

    @Override
    void close();

}
