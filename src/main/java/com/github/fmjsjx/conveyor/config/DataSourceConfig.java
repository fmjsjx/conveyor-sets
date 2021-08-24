package com.github.fmjsjx.conveyor.config;

public interface DataSourceConfig {

    String jdbcUrl();

    String username();

    String password();
    
    String serverTimezone();
    
    boolean useSSL();

    default String poolName() {
        return jdbcUrl() + "@" + username();
    }

}
