package com.github.fmjsjx.conveyor.config;

public interface DataSourceConfig {

    String poolName();

    String jdbcUrl();

    String username();

    String password();

    String serverTimezone();

    boolean useSSL();

}
