package com.github.fmjsjx.conveyor.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class ConveyorSetConfigTest {

    @Test
    public void test() {
        try (var in = getClass().getResourceAsStream("/conf.d/cowboy-1038.yml")) {
            var config = ConveyorSetConfig.loadFromYaml(in);
            assertNotNull(config);
            assertEquals("cowboy-1038", config.name);
            assertEquals(1, config.products.size());
            assertEquals(1038, config.products.get(0));
            assertEquals(3, config.conveyors.size());
            assertEquals("device", config.conveyors.get(0).name);
            assertEquals("event", config.conveyors.get(1).name);
            assertEquals("item", config.conveyors.get(2).name);
        } catch (Exception e) {
            fail(e);
        }
    }
    
}
