package com.github.fmjsjx.conveyor.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.fmjsjx.conveyor.config.ConveyorSetConfig;
import com.github.fmjsjx.conveyor.config.ConveyorSetsConfig;
import com.github.fmjsjx.conveyor.config.InputRedisConfig;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig;
import com.github.fmjsjx.conveyor.config.InputRedisConfig.Type;
import com.github.fmjsjx.conveyor.core.Conveyor;
import com.github.fmjsjx.conveyor.core.ConveyorSet;
import com.github.fmjsjx.conveyor.core.DefaultConveyor;
import com.github.fmjsjx.conveyor.core.DefaultConveyorSet;
import com.github.fmjsjx.conveyor.core.input.Input;
import com.github.fmjsjx.conveyor.core.input.RedisListInput;
import com.github.fmjsjx.conveyor.core.input.RedisStreamInput;
import com.github.fmjsjx.conveyor.core.output.DerivedMysqlOutput;
import com.github.fmjsjx.conveyor.core.output.Output;
import com.github.fmjsjx.conveyor.core.output.SimpleMysqlOutput;
import com.github.fmjsjx.conveyor.util.ConfigUtil;
import com.github.fmjsjx.libcommon.collection.CollectorUtil;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ConveyorSetManager implements InitializingBean, DisposableBean {

    @Autowired
    private DataSourceManager dataSourceManager;
    @Autowired
    private RedisClientManager redisClientManager;

    private final ConcurrentMap<String, ConveyorSet> conveyorSetMap = new ConcurrentHashMap<>();

    private ExecutorService executor;

    @Override
    public void afterPropertiesSet() throws Exception {
        // load configurations
        var mainCfg = ConfigUtil.loadConfiguration("conveyor-sets.yml", ConveyorSetsConfig::loadFromYaml);
        var conveyorSetCfgs = new ArrayList<ConveyorSetConfig>();
        var nameSet = new HashSet<String>();
        for (var include : mainCfg.includes()) {
            var paths = ConfigUtil.searchFiles(include);
            for (var path : paths) {
                var conveyorSetCfg = ConfigUtil.loadConfiguration(path.toFile(), ConveyorSetConfig::loadFromYaml);
                validate(conveyorSetCfg);
                if (!nameSet.add(conveyorSetCfg.name())) {
                    throw new IllegalArgumentException(
                            "duplicated name `" + conveyorSetCfg.name() + "` for conveyor sets on file " + path);
                }
                conveyorSetCfgs.add(conveyorSetCfg);
            }
        }
        var conveyorSetMap = this.conveyorSetMap;
        for (var conveyorSetCfg : conveyorSetCfgs) {
            conveyorSetMap.put(conveyorSetCfg.name(), initConveyorSet(conveyorSetCfg));
        }
        // startup
        var executor = this.executor = Executors.newCachedThreadPool(new DefaultThreadFactory("conveyor"));
        for (var conveyorSet : conveyorSetMap.values()) {
            conveyorSet.startup(executor);
        }
    }

    private void validate(ConveyorSetConfig conveyorSetCfg) {
        var consumerSet = new HashSet<String>();
        for (var conveyorCfg : conveyorSetCfg.conveyors()) {
            var inputRedis = conveyorCfg.inputRedis();
            if (inputRedis.type() == Type.STREAM) {
                // validate stream arguments
                var stream = inputRedis.stream();
                var consumer = inputRedis.key() + ".groups[\"" + stream.group() + "\"]=" + stream.consumer();
                if (!consumerSet.add(consumer)) {
                    throw new UnsupportedOperationException(
                            "duplicated consumer `" + consumer + "` for redis.input.stream");
                }
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        // Shutdown all ConveyorSets
        var futures = conveyorSetMap.values().stream()
                .collect(CollectorUtil.toLinkedHashMap(ConveyorSet::name, ConveyorSet::shutdown));
        futures.forEach((k, v) -> {
            if (!v.awaitUninterruptibly(60, TimeUnit.SECONDS)) {
                var conveyorSet = conveyorSetMap.get(k);
                if (!conveyorSet.isTerminated()) {
                    log.warn("[app:shutdown] {} may not terminated properly", conveyorSet);
                }
            }
        });
        // release all resources
        var executor = this.executor;
        if (executor != null) {
            log.info("[app:shutdown] Shutdown conveyor executor");
            executor.shutdown();
            if (!executor.isTerminated()) {
                executor.awaitTermination(60, TimeUnit.SECONDS);
            }
        }
        redisClientManager.shutdown();
        dataSourceManager.closeAll();
    }

    private ConveyorSet initConveyorSet(ConveyorSetConfig config) {
        log.debug("[app:init] Initialize conveyor set by {}", config);
        var conveyors = new ArrayList<Conveyor>();
        for (var productId : config.products()) {
            for (var conveyorCfg : config.conveyors()) {
                var conveyorName = config.name() + "-" + productId + "." + conveyorCfg.name();
                var input = initRedisInput(productId, conveyorName, conveyorCfg.inputRedis());
                var output = initMysqlOutput(conveyorName, conveyorCfg.outputMysql());
                var conveyor = new DefaultConveyor(conveyorName, input, output);
                log.debug("[app:init] {} initialized", conveyor);
                conveyors.add(conveyor);
            }
        }
        var conveyorSet = new DefaultConveyorSet(config.name(), conveyors);
        log.debug("[app:init] {} initialized", conveyorSet);
        return conveyorSet;
    }

    private Output initMysqlOutput(String conveyorName, OutputMysqlConfig outputMysql) {
        var outputName = conveyorName + ".output.mysql";
        var dataSource = dataSourceManager.getDataSource(outputMysql);
        if (outputMysql.derivationEnabled()) {
            return new DerivedMysqlOutput(outputName, dataSource, outputMysql);
        } else {
            return new SimpleMysqlOutput(outputName, dataSource, outputMysql);
        }
    }

    private Input initRedisInput(Integer productId, String conveyorName, InputRedisConfig config) {
        var inputName = conveyorName + ".input.redis";
        return switch (config.type()) {
        case LIST -> new RedisListInput(inputName, redisClientManager.globalRedisClient(), productId, config);
        case STREAM -> new RedisStreamInput(inputName, redisClientManager.globalRedisClient(), productId, config);
        default -> throw new UnsupportedOperationException("type " + config.type() + " is unsupported yet");
        };
    }

}
