package com.github.fmjsjx.conveyor.core.input;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.github.fmjsjx.conveyor.config.InputRedisConfig;
import com.github.fmjsjx.libcommon.redis.LuaScript;
import com.github.fmjsjx.libcommon.redis.RedisUtil;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisStreamInput extends AbstractRedisInput {

    private static final LuaScript<Long> INIT_STREAM_GROUP = LuaScript.forNumber("""
            local key = KEYS[1]
            local stream_group = ARGV[1]
            local need_create = 1
            if (redis.call('exists', key) == 1) then
              local groups = redis.call('xinfo', 'groups', key)
              need_create = 1
              for i=1,#groups do
                local group = groups[i]
                for j=1,#group/2 do
                  local field = group[(2 * j) - 1]
                  local value = group[2 * j]
                  if (field == 'name' and value == stream_group) then
                    need_create = 0
                    break
                  end
                end
                if (need_create == 0) then
                  break
                end
              end
            end
            if (need_create == 1) then
              redis.call('xgroup', 'create', key, stream_group, '0', 'mkstream')
            end
            return need_create
            """);

    private final int batch;
    private final Consumer<String> consumer;
    private final XReadArgs blockingArgs;
    private final XReadArgs batchArgs;
    private final StreamOffset<String>[] streams;

    private boolean initialized;
    private boolean blocking;

    @SuppressWarnings("unchecked")
    public RedisStreamInput(String name, RedisClient client, int productId, InputRedisConfig config) {
        super(name, client, productId, config);
        batch = config.batch();
        consumer = Consumer.from(config.stream().group(), config.stream().consumer());
        blockingArgs = new XReadArgs().count(batch).block(Duration.ofSeconds(5));
        batchArgs = new XReadArgs().count(batch);
        streams = new StreamOffset[] { StreamOffset.lastConsumed(key) };
    }

    @Override
    public List<Map<String, String>> fetch() {
        if (!initialized) {
            init();
            initialized = true;
        }
        if (blocking) {
            var messages = xreadgroup(blockingArgs);
            if (messages.isEmpty()) {
                return List.of();
            }
            blocking = true;
            switch (messages.size()) {
            case 1:
                return List.of(messages.get(0).getBody());
            case 2:
                return List.of(messages.get(0).getBody(), messages.get(1).getBody());
            default:
                return messages.stream().map(StreamMessage::getBody).toList();
            }
        }
        var messages = xreadgroup(batchArgs);
        if (messages.size() < batch) {
            blocking = true;
        }
        switch (messages.size()) {
        case 0:
            return List.of();
        case 1:
            return List.of(messages.get(0).getBody());
        case 2:
            return List.of(messages.get(0).getBody(), messages.get(1).getBody());
        default:
            return messages.stream().map(StreamMessage::getBody).toList();
        }

    }

    private List<StreamMessage<String, String>> xreadgroup(XReadArgs xreadArgs) {
        try {
            return redisSync().xreadgroup(consumer, xreadArgs, streams);
        } catch (RedisCommandExecutionException e) {
            if (e.getMessage().startsWith("NOGROUP")) {
                log.warn("[redis:input] {}", e.getMessage(), e);
                init();
                return xreadgroup(xreadArgs);
            } else {
                throw e;
            }
        }
    }

    private void init() {
        var keys = new String[] { streams[0].getName() };
        var value = consumer.getGroup();
        log.info("[input:redis] Initialize XSTREAM group {} {}", keys[0], value);
        log.debug("[input:redis] EVAL {} 1 {} {}", INIT_STREAM_GROUP, keys, value);
        RedisUtil.eval(redisSync(), INIT_STREAM_GROUP, keys, value);
    }

}
