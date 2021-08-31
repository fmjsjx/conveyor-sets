package com.github.fmjsjx.conveyor.core.output;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.FieldConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.FieldType;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.InsertMode;
import com.github.fmjsjx.conveyor.util.LoggerUtil;
import com.github.fmjsjx.libcommon.json.Jackson2Library;

import io.netty.util.collection.IntObjectHashMap;

public abstract class AbstractMysqlOutput implements Output {

    protected static final String toSqlColumns(List<FieldConfig> fields) {
        return fields.stream().map(FieldConfig::fixedColumn).collect(Collectors.joining(","));
    }

    protected static final String toSqlPlaceholders(List<FieldConfig> fields) {
        var placeholders = fields.stream().map(FieldConfig::type).map(FieldType::placeholder)
                .collect(Collectors.joining(","));
        return "(" + placeholders + ")";
    }

    protected static final IntFunction<String> toBatchSqlFactory(InsertMode mode, String table, String columns,
            String placeholders) {
        var sqlPrefix = mode.toSql(table) + " (" + columns + ") VALUES ";
        var batchSqlMap = new IntObjectHashMap<String>(16);
        return batch -> {
            var sql = batchSqlMap.get(batch);
            if (sql == null) {
                var values = new String[batch];
                Arrays.fill(values, placeholders);
                sql = sqlPrefix + String.join(",", values);
                batchSqlMap.put(batch, sql);
            }
            return sql;
        };
    }

    protected final String name;
    protected final DataSource dataSource;

    protected AbstractMysqlOutput(String name, DataSource dataSource) {
        this.name = name;
        this.dataSource = dataSource;
    }

    @Override
    public void close() {
        // default do nothing
    }

    @Override
    public void failed(List<Map<String, String>> batch) {
        var failureLogger = LoggerUtil.failureLogger();
        var name = this.name;
        for (var values : batch) {
            var value = Jackson2Library.getInstance().dumpsToString(values);
            failureLogger.info("{} primary --- {}", name, value);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + name + ")";
    }

}
