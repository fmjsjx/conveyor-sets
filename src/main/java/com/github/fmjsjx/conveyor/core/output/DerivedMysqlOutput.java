package com.github.fmjsjx.conveyor.core.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.github.fmjsjx.conveyor.config.OutputMysqlConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.FieldConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.ValueSetter;
import com.github.fmjsjx.conveyor.util.LoggerUtil;
import com.github.fmjsjx.libcommon.json.Jackson2Library;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DerivedMysqlOutput extends AbstractMysqlOutput {

    final Map<String, String> derivedTables;
    final IntFunction<String> batchSqlFactory;
    final List<ValueSetter> valueSetters;
    final String derivedField;
    final Map<String, IntFunction<String>> derivedBatchSqlFactories = new HashMap<>();

    public DerivedMysqlOutput(String name, DataSource dataSource, OutputMysqlConfig config) {
        super(name, dataSource);
        var columns = toSqlColumns(config.fields());
        var placeholders = toSqlPlaceholders(config.fields());
        var mode = config.mode();
        batchSqlFactory = toBatchSqlFactory(mode, config.fixedTable(), columns, placeholders);
        valueSetters = config.fields().stream().map(FieldConfig::valueSetter).collect(Collectors.toUnmodifiableList());
        derivedField = config.derivedField();
        derivedTables = config.derivedTables();
        config.derivedTables().forEach((k, v) -> derivedBatchSqlFactories.put(k,
                toBatchSqlFactory(mode, config.fixedTable(v), columns, placeholders)));
    }

    @Override
    public void push(List<Map<String, String>> batch) throws Exception {
        try (var conn = dataSource.getConnection()) {
            var derivedMap = new LinkedHashMap<String, List<Map<String, String>>>();
            var sql = batchSqlFactory.apply(batch.size());
            var valueSetters = this.valueSetters;
            var derivedBatchSqlFactories = this.derivedBatchSqlFactories;
            try (var statement = conn.prepareStatement(sql)) {
                var index = 1;
                var derivedField = this.derivedField;
                for (var values : batch) {
                    var df = values.get(derivedField);
                    if (df != null && derivedBatchSqlFactories.containsKey(df)) {
                        derivedMap.computeIfAbsent(df, k -> new ArrayList<>()).add(values);
                    }
                    for (var setter : valueSetters) {
                        setter.setValue(statement, index++, values);
                    }
                }
                log.debug("[output:mysql] {} ==> {}", sql, batch);
                statement.executeUpdate();
            }
            if (derivedMap.size() > 0) {
                for (var entry : derivedMap.entrySet()) {
                    var key = entry.getKey();
                    var derivedBatch = entry.getValue();
                    var derivedSql = derivedBatchSqlFactories.get(key).apply(derivedBatch.size());
                    try (var statement = conn.prepareStatement(derivedSql)) {
                        var index = 1;
                        for (var values : derivedBatch) {
                            for (var setter : valueSetters) {
                                setter.setValue(statement, index++, values);
                            }
                        }
                        log.debug("[outout:mysql] {} ==> {}", derivedSql, derivedBatch);
                        statement.executeUpdate();
                    } catch (Exception e) {
                        log.error("[outout:mysql] Push derived table failed: {} => {}", key, derivedSql, e);
                        var failureLogger = LoggerUtil.failureLogger();
                        var name = this.name;
                        for (var values : derivedBatch) {
                            var value = Jackson2Library.getInstance().dumpsToString(values);
                            failureLogger.info("{} derived {} {}", name, key, value);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "DerivedMysqlOutput(" + name + ")";
    }

}
