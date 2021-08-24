package com.github.fmjsjx.conveyor.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.github.fmjsjx.conveyor.config.DataSourceConfig;
import com.github.fmjsjx.libcommon.util.RuntimeUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DataSourceManager {

    private final ConcurrentMap<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    public DataSource getDataSource(DataSourceConfig config) {
        if (closed.get()) {
            throw new IllegalStateException("already closed");
        }
        return dataSourceMap.computeIfAbsent(config.poolName(), k -> createDataSource(config));
    }

    private DataSource createDataSource(DataSourceConfig config) {
        // now always use HikariCP
        return createHikariDataSource(config);
    }

    private HikariDataSource createHikariDataSource(DataSourceConfig config) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName(config.poolName());
        cfg.setJdbcUrl(config.jdbcUrl());
        cfg.setUsername(config.username());
        cfg.setPassword(config.password());
        cfg.addDataSourceProperty("serverTimezone", config.serverTimezone());
        cfg.addDataSourceProperty("useUnicode", "true");
        cfg.addDataSourceProperty("characterEncoding", "UTF-8");
        cfg.addDataSourceProperty("useSSL", config.useSSL() ? "true" : "false");
        cfg.setAutoCommit(true);
        cfg.setConnectionTestQuery("SELECT 'x'");
        cfg.setMinimumIdle(Math.max(1, RuntimeUtil.availableProcessors() / 2));
        cfg.setMaximumPoolSize(64);
        var dataSource = new HikariDataSource(cfg);
        log.info("[data-source:create] {} created", dataSource);
        return dataSource;
    }

    public void closeAll() {
        if (closed.compareAndSet(false, true)) {
            log.info("[app:shutdown] Close all data sources");
            dataSourceMap.values().forEach(dataSource -> {
                if (dataSource instanceof HikariDataSource) {
                    log.info("[app:shutdown] Close {}", dataSource);
                    ((HikariDataSource) dataSource).close();
                    log.info("[app:shutdown] {} closed", dataSource);
                } else {
                    log.warn("[app:shutdown] Try to close unknown data source {}", dataSource);
                    safeClose(dataSource);
                }
            });
            log.info("[app:shutdown] All data source closed.");
        }
    }

    private static final void safeClose(DataSource dataSource) {
        try {
            var method = dataSource.getClass().getMethod("close");
            method.invoke(dataSource);
            log.warn("[app:shutdown] {} closed", dataSource);
        } catch (Exception e) {
            log.warn("[app:shutdown] Close {} failed", dataSource, e);
        }
    }

}
