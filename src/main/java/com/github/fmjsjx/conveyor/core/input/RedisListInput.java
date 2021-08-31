package com.github.fmjsjx.conveyor.core.input;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.github.fmjsjx.conveyor.config.InputRedisConfig;
import com.github.fmjsjx.libcommon.json.JsoniterLibrary;
import com.github.fmjsjx.libcommon.redis.LuaScript;
import com.github.fmjsjx.libcommon.redis.RedisUtil;
import com.jsoniter.spi.TypeLiteral;

import io.lettuce.core.RedisClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisListInput extends AbstractRedisInput {

    private static final LuaScript<List<String>> RPOP_N = LuaScript.forList("""
            local key = KEYS[1]
            local limit = tonumber(ARGV[1])
            local arr = {}
            for i=1,limit do
              local v = redis.call('rpop', key)
              if (not(v)) then
                break
              end
              table.insert(arr, v)
            end
            return arr
            """);

    private static final TypeLiteral<Map<String, String>> MAP_TYPE_LITERAL = new TypeLiteral<>() {};

    private static final Map<String, String> toData(String value) {
        try {
            return JsoniterLibrary.getInstance().loads(value, MAP_TYPE_LITERAL);
        } catch (Exception e) {
            log.warn("[input:redis] parse data failed: {}", value, e);
            return null;
        }
    }

    private final int batch;
    private final String[] keys;
    private final String batchArg;

    private boolean blocking;

    public RedisListInput(String name, RedisClient client, String productId, InputRedisConfig config) {
        super(name, client, productId, config);
        this.batch = config.batch();
        this.keys = new String[] { key };
        this.batchArg = String.valueOf(batch);
    }

    @Override
    public List<Map<String, String>> fetch() {
        if (blocking) {
            var kv = redisSync().brpop(5, key);
            if (kv == null) {
                return List.of();
            }
            blocking = false;
            var data = toData(kv.getValue());
            return data == null ? List.of() : List.of(data);
        }
        var values = RedisUtil.eval(redisSync(), RPOP_N, keys, batchArg);
        if (values.size() < batch) {
            blocking = true;
        }
        switch (values.size()) {
        case 0:
            return List.of();
        case 1: {
            var data = toData(values.get(0));
            return data == null ? List.of() : List.of(data);
        }
        case 2: {
            var data0 = toData(values.get(0));
            var data1 = toData(values.get(1));
            if (data0 != null) {
                if (data1 != null) {
                    return List.of(data0, data1);
                }
                return List.of(data0);
            }
            if (data1 != null) {
                return List.of(data1);
            }
            return List.of();
        }
        default:
            return values.stream().map(RedisListInput::toData).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }

}
