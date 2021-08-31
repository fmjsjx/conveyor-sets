package com.github.fmjsjx.conveyor.config;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fmjsjx.conveyor.util.CollectionUtil;
import com.github.fmjsjx.libcommon.util.DateTimeUtil;
import com.github.fmjsjx.libcommon.util.StringUtil;

import lombok.ToString;

@ToString
public class OutputMysqlConfig implements DataSourceConfig {

    private static final ValueSetter generateValueSetter(String name, FieldType type, DefaultValueConfig def) {
        switch (type) {
        case DATE:
            return generateDateSetter(name, def);
        case DATETIME:
            return generateDatetimeSetter(name, def);
        case INT:
            return generateIntSetter(name, def);
        case JSON_OBJECT:
            return generateJsonObjectSetter(name, def);
        case LONG:
            return generateLongSetter(name, def);
        default:
        case STRING:
            return generateStringSetter(name, def);
        case UNIX_TIME:
            return generateUnixTimeSetter(name, def);
        }
    }

    private static final ValueSetter generateDateSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            if ("today".equalsIgnoreCase(def.content())) {
                return (statement, index, values) -> {
                    var value = values.get(name);
                    if (value == null) {
                        statement.setDate(index, Date.valueOf(LocalDate.now()));
                    } else {
                        statement.setDate(index, Date.valueOf(value));
                    }
                };
            }
            var defaultValue = Date.valueOf(def.content());
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setDate(index, defaultValue);
                } else {
                    statement.setDate(index, Date.valueOf(value));
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            if (value == null) {
                statement.setObject(index, null);
            } else {
                statement.setDate(index, Date.valueOf(value));
            }
        };
    }

    private static final ValueSetter generateDatetimeSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            if ("now".equalsIgnoreCase(def.content())) {
                return (statement, index, values) -> {
                    var value = values.get(name);
                    if (value == null) {
                        statement.setObject(index, LocalDateTime.now());
                    } else {
                        statement.setObject(index, LocalDateTime.parse(value));
                    }
                };
            }
            var defaultValue = LocalDateTime.parse(def.content());
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setObject(index, defaultValue);
                } else {
                    statement.setObject(index, LocalDateTime.parse(value));
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            if (value == null) {
                statement.setObject(index, null);
            } else {
                statement.setObject(index, LocalDateTime.parse(value));
            }
        };
    }

    private static final ValueSetter generateIntSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            var defaultValue = Integer.parseInt(def.content());
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setInt(index, defaultValue);
                } else {
                    statement.setInt(index, Integer.parseInt(value));
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            if (value == null) {
                statement.setObject(index, null);
            } else {
                statement.setInt(index, Integer.parseInt(value));
            }
        };
    }

    private static final ValueSetter generateJsonObjectSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            var defaultValue = StringUtil.isBlank(def.content()) ? "{}" : def.content();
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setString(index, defaultValue);
                } else {
                    statement.setString(index, value);
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            statement.setString(index, value);
        };
    }

    private static final ValueSetter generateLongSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            var defaultValue = Long.parseLong(def.content());
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setLong(index, defaultValue);
                } else {
                    statement.setLong(index, Long.parseLong(value));
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            if (value == null) {
                statement.setObject(index, null);
            } else {
                statement.setLong(index, Long.parseLong(value));
            }
        };
    }

    private static final ValueSetter generateStringSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            var defaultValue = def.content();
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setString(index, defaultValue);
                } else {
                    statement.setString(index, value);
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            statement.setString(index, value);
        };
    }

    private static final ValueSetter generateUnixTimeSetter(String name, DefaultValueConfig def) {
        if (def.isPresent()) {
            if ("now".equalsIgnoreCase(def.content())) {
                return (statement, index, values) -> {
                    var value = values.get(name);
                    if (value == null) {
                        statement.setDouble(index, DateTimeUtil.unixTimeWithMs());
                    } else {
                        statement.setDouble(index, Double.parseDouble(value));
                    }
                };
            }
            var defaultValue = LocalDateTime.parse(def.content());
            return (statement, index, values) -> {
                var value = values.get(name);
                if (value == null) {
                    statement.setObject(index, defaultValue);
                } else {
                    statement.setDouble(index, Double.parseDouble(value));
                }
            };
        }
        return (statement, index, values) -> {
            var value = values.get(name);
            if (value == null) {
                statement.setObject(index, null);
            } else {
                statement.setDouble(index, Double.parseDouble(value));
            }
        };
    }

    private static final String toPoolName(String jdbcUrl, String username, String password, String serverTimezone,
            boolean useSSL) {
        return username + ":" + password + "@" + jdbcUrl + "?" + serverTimezone + (useSSL ? "&useSSL" : "");
    }

    public enum InsertMode {
        IGNORE(t -> "INSERT IGNORE INTO " + t), REPLACE(t -> "REPLACE INFO " + t);

        private final Function<String, String> sqlFactory;

        private InsertMode(Function<String, String> sqlFactory) {
            this.sqlFactory = sqlFactory;
        }

        public String toSql(String table) {
            return sqlFactory.apply(table);
        }

    }

    public enum FieldType {
        INT, LONG, STRING, UNIX_TIME("from_unixtime(?)"), DATETIME, DATE, JSON_OBJECT;

        private final String placeholder;

        private FieldType(String placeholder) {
            this.placeholder = placeholder;
        }

        private FieldType() {
            this("?");
        }

        public String placeholder() {
            return placeholder;
        }

    }

    @ToString
    public static final class FieldConfig {

        final String name;
        final String column;
        final FieldType type;
        final DefaultValueConfig defaultValue;
        final ValueSetter valueSetter;

        @JsonCreator
        public FieldConfig(@JsonProperty(value = "name", required = true) String name,
                @JsonProperty(value = "column", required = true) String column,
                @JsonProperty(value = "type", required = true) String type,
                @JsonProperty(value = "default", required = false) String defaultValue) {
            this.name = name;
            this.column = column;
            this.type = FieldType.valueOf(type.toUpperCase());
            this.defaultValue = new DefaultValueConfig(defaultValue);
            this.valueSetter = generateValueSetter(name, this.type, this.defaultValue);
        }

        public String name() {
            return name;
        }

        public String column() {
            return column;
        }

        public String fixedColumn() {
            return "`" + column + "`";
        }

        public FieldType type() {
            return type;
        }

        public DefaultValueConfig defaultValue() {
            return defaultValue;
        }

        public ValueSetter valueSetter() {
            return valueSetter;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof FieldConfig) {
                var o = (FieldConfig) obj;
                return name.equals(o.name) && column.equals(o.column) && type == o.type
                        && defaultValue.equals(o.defaultValue);
            }
            return false;
        }

    }

    public interface ValueSetter {

        void setValue(PreparedStatement statement, int index, Map<String, String> values) throws SQLException;

    }

    final String poolName;
    final String jdbcUrl;
    final String username;
    final String password;
    final String serverTimezone;
    final boolean useSSL;
    final Optional<String> schema;
    final String table;
    final InsertMode mode;
    final boolean derivationEnabled;
    final String derivedField;
    final Map<String, String> derivedTables;
    final List<FieldConfig> fields;

    @JsonCreator
    public OutputMysqlConfig(@JsonProperty(value = "jdbc-url", required = true) String jdbcUrl,
            @JsonProperty(value = "username", required = true) String username,
            @JsonProperty(value = "password", required = true) String password,
            @JsonProperty(value = "server-timezone", required = false) String serverTimezone,
            @JsonProperty(value = "use-ssl", required = false) boolean useSSL,
            @JsonProperty(value = "schema", required = false) String schema,
            @JsonProperty(value = "table", required = true) String table,
            @JsonProperty(value = "mode", required = false) String mode,
            @JsonProperty(value = "derivation-enabled", required = false) boolean derivationEnabled,
            @JsonProperty(value = "derived-field", required = false) String derivedField,
            @JsonProperty(value = "derived-tables", required = false) Map<String, String> derivedTables,
            @JsonProperty(value = "fields", required = true) List<FieldConfig> fields) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.serverTimezone = Optional.ofNullable(serverTimezone).orElse("Asia/Shanghai");
        this.useSSL = useSSL;
        this.poolName = toPoolName(jdbcUrl, username, password, serverTimezone, useSSL);
        this.schema = Optional.ofNullable(schema);
        this.table = table;
        this.mode = InsertMode.valueOf(Optional.ofNullable(mode).orElse("ignore").toUpperCase());
        this.derivationEnabled = derivationEnabled;
        if (derivationEnabled) {
            this.derivedField = Objects.requireNonNull(derivedField, "derived-field");
            this.derivedTables = derivedTables == null ? Map.of() : Map.copyOf(derivedTables);
        } else {
            this.derivedField = null;
            this.derivedTables = null;
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("empty fields");
        }
        this.fields = List.copyOf(fields);
    }

    @Override
    public String poolName() {
        return poolName;
    }

    @Override
    public String jdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public String serverTimezone() {
        return serverTimezone;
    }

    @Override
    public boolean useSSL() {
        return useSSL;
    }

    public Optional<String> schema() {
        return schema;
    }

    public String table() {
        return table;
    }

    public String fixedTable(String table) {
        if (schema.isPresent()) {
            return "`" + schema.get() + "`.`" + table + "`";
        }
        return "`" + table + "`";
    }

    public String fixedTable() {
        return fixedTable(table);
    }

    public InsertMode mode() {
        return mode;
    }

    public boolean derivationEnabled() {
        return derivationEnabled;
    }

    public String derivedField() {
        return derivedField;
    }

    public Map<String, String> derivedTables() {
        return derivedTables;
    }

    public List<FieldConfig> fields() {
        return fields;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof OutputMysqlConfig) {
            var o = (OutputMysqlConfig) obj;
            if (poolName.equals(o.poolName) && schema.equals(o.schema) && table.equals(o.table) && mode == o.mode
                    && derivationEnabled == o.derivationEnabled) {
                if (CollectionUtil.isEqual(fields, o.fields)) {
                    if (derivationEnabled) {
                        return derivedField.equals(o.derivedField) && derivedTables.equals(o.derivedTables);
                    }
                    return true;
                }
            }
        }
        return false;
    }

}
