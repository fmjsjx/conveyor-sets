package com.github.fmjsjx.conveyor.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

public class ConveyorSetsConfigTest {

    @Test
    public void testLoadFromYaml() {
        try (var in = getClass().getResourceAsStream("/conf/conveyor-sets.yml")) {
            var config = ConveyorSetsConfig.loadFromYaml(in);
            assertNotNull(config);
            assertNotNull(config.includes);
            assertEquals(1, config.includes.size());
            assertEquals("../conf.d/*.yml", config.includes.get(0));
        } catch (Exception e) {
            fail(e);
        }
    }
    
}
