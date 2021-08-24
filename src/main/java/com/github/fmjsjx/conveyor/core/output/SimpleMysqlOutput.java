package com.github.fmjsjx.conveyor.core.output;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.github.fmjsjx.conveyor.config.OutputMysqlConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.FieldConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig.ValueSetter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleMysqlOutput extends AbstractMysqlOutput {

    final IntFunction<String> batchSqlFactory;
    final List<ValueSetter> valueSetters;

    public SimpleMysqlOutput(String name, DataSource dataSource, OutputMysqlConfig config) {
        super(name, dataSource);
        var columns = toSqlColumns(config.fields());
        var placeholders = toSqlPlaceholders(config.fields());
        batchSqlFactory = toBatchSqlFactory(config.mode(), config.table(), columns, placeholders);
        valueSetters = config.fields().stream().map(FieldConfig::valueSetter).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void push(List<Map<String, String>> batch) throws Exception {
        try (var conn = dataSource.getConnection()) {
            var sql = batchSqlFactory.apply(batch.size());
            log.debug("[output:mysql] {} ==> {}", sql, batch);
            var valueSetters = this.valueSetters;
            try (var statement = conn.prepareStatement(sql)) {
                var index = 1;
                for (var values : batch) {
                    for (var setter : valueSetters) {
                        setter.setValue(statement, index++, values);
                    }
                }
                statement.executeUpdate();
            }
        }
    }

}
