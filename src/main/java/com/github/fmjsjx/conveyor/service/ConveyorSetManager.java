package com.github.fmjsjx.conveyor.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.fmjsjx.conveyor.AppProperties;
import com.github.fmjsjx.conveyor.admin.AdminServer;
import com.github.fmjsjx.conveyor.admin.Resp3Server;
import com.github.fmjsjx.conveyor.admin.UnixServer;
import com.github.fmjsjx.conveyor.config.ConveyorSetConfig;
import com.github.fmjsjx.conveyor.config.ConveyorSetsConfig;
import com.github.fmjsjx.conveyor.config.InputRedisConfig;
import com.github.fmjsjx.conveyor.config.InputRedisConfig.Type;
import com.github.fmjsjx.conveyor.config.OutputMysqlConfig;
import com.github.fmjsjx.conveyor.config.Resp3ServerConfig;
import com.github.fmjsjx.conveyor.config.UnixServerConfig;
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

import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ConveyorSetManager implements InitializingBean, DisposableBean {

    @Autowired
    private AppProperties appProperties;
    @Autowired
    private DataSourceManager dataSourceManager;
    @Autowired
    private RedisClientManager redisClientManager;

    private final ConcurrentMap<String, ConveyorSet> conveyorSetMap = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newCachedThreadPool(new DefaultThreadFactory("conveyor"));

    private final ExecutorService adminExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("admin"));

    private final List<AdminServer> adminServers = new ArrayList<>();

    @Override
    public void afterPropertiesSet() throws Exception {
        var mainCfg = loadConfigurations();
        startup();
        startupAdminServers(mainCfg);
    }

    private ConveyorSetsConfig loadConfigurations() throws Exception {
        var mainCfg = loadMainCfg();
        var conveyorSetCfgs = loadIncludes(mainCfg);
        var conveyorSetMap = this.conveyorSetMap;
        for (var conveyorSetCfg : conveyorSetCfgs) {
            var conveyorSet = initConveyorSet(conveyorSetCfg);
            if (conveyorSetCfg.autoStart()) {
                conveyorSetMap.put(conveyorSetCfg.name(), conveyorSet);
            }
        }
        return mainCfg;
    }

    private ConveyorSetsConfig loadMainCfg() throws FileNotFoundException, IOException {
        log.info("[app:init] Loading configuration conveyor-sets.yml");
        var mainCfg = ConfigUtil.loadConfiguration("conveyor-sets.yml", ConveyorSetsConfig::loadFromYaml);
        log.debug("[app:init] Loaded configuration conveyor-sets.yml <== {}", mainCfg);
        return mainCfg;
    }

    private List<ConveyorSetConfig> loadIncludes(ConveyorSetsConfig mainCfg) throws IOException, FileNotFoundException {
        var conveyorSetCfgs = new ArrayList<ConveyorSetConfig>();
        var nameSet = new HashSet<String>();
        for (var include : mainCfg.includes()) {
            var paths = ConfigUtil.searchFiles(include);
            for (var path : paths) {
                log.info("[app:init] Loading configuration {}", path);
                var conveyorSetCfg = ConfigUtil.loadConfiguration(path.toFile(), ConveyorSetConfig::loadFromYaml);
                log.debug("[app:init] Loaded configuration {} <== {}", path, conveyorSetCfg);
                validate(conveyorSetCfg);
                if (!nameSet.add(conveyorSetCfg.name())) {
                    throw new IllegalArgumentException(
                            "duplicated name `" + conveyorSetCfg.name() + "` for conveyor sets on file " + path);
                }
                conveyorSetCfgs.add(conveyorSetCfg);
            }
        }
        return conveyorSetCfgs;
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

    private void startup() throws Exception {
        var conveyorSetMap = this.conveyorSetMap;
        var cd = new CountDownLatch(conveyorSetMap.size());
        for (var conveyorSet : conveyorSetMap.values()) {
            conveyorSet.startup(executor).addListener(cs -> cd.countDown());
        }
        cd.await();
    }

    private void startupAdminServers(ConveyorSetsConfig mainCfg) {
        mainCfg.unixServer().ifPresent(this::startupUnixServer);
        mainCfg.resp3Server().ifPresent(this::startupResp3Server);
    }

    private void startupUnixServer(UnixServerConfig config) {
        if (Epoll.isAvailable()) {
            adminServers.add(new UnixServer(this, config));
        }
    }

    private void startupResp3Server(Resp3ServerConfig config) {
        adminServers.add(new Resp3Server(appProperties.getVersion(), this, config));
    }

    @Override
    public void destroy() throws Exception {
        // Shutdown admin executor at first
        var adminExecutor = this.adminExecutor;
        log.info("[app:shutdown] Shutdown admin executor");
        adminExecutor.shutdown();
        if (!executor.isTerminated()) {
            adminExecutor.awaitTermination(60, TimeUnit.SECONDS);
        }
        // Shutdown all admin servers
        adminServers.forEach(AdminServer::shutdown);
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
        log.info("[app:shutdown] Shutdown conveyor executor");
        executor.shutdown();
        if (!executor.isTerminated()) {
            executor.awaitTermination(60, TimeUnit.SECONDS);
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
        var conveyorSet = new DefaultConveyorSet(config, conveyors);
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

    public String adminCommandLine(String line) {
        try {
            return CompletableFuture.supplyAsync(() -> commandLine(line), adminExecutor).get();
        } catch (Exception e) {
            log.error("[admin:command] execute command line failed: `{}`", line, e);
            return e.toString();
        }
    }

    private String commandLine(String line) {
        var args = line.split("\\s+", 2);
        var cmd = args[0];
        switch (cmd) {
        case "status":
            return showStatus(args);
        case "reread":
            return reread();
        case "start":
            if (args.length == 1) {
                return startAll();
            }
            return start(args[0]);
        case "stop":
            if (args.length == 1) {
                return stopAll();
            }
            return stop(args[0]);
        case "restart":
            if (args.length == 1) {
                return restartAll();
            }
            return restart(args[0]);
        case "update":
            if (args.length == 1) {
                return updateAll();
            }
            return update(args[0]);
        default:
            return "unknown command `" + cmd + "`";
        }
    }

    private String showStatus(String[] args) {
        if (args.length > 1) {
            var name = args[1];
            var conveyorSet = conveyorSetMap.get(name);
            if (conveyorSet == null) {
                return name + ": ERROR (no such conveyor-set)";
            }
            return StatusUtil.showStatus(conveyorSet);
        }
        var conveyorSets = conveyorSetMap.values().stream().toList();
        var nameWidth = conveyorSets.stream().mapToInt(cs -> StatusUtil.toNameWidth(cs.name())).max().orElse(30);
        return conveyorSets.stream().map(cs -> StatusUtil.format(nameWidth, cs)).collect(Collectors.joining("\r\n"));
    }

    private List<ConveyorSetConfig> loadIncludes() throws Exception {
        ConveyorSetsConfig mainCfg;
        try {
            mainCfg = loadMainCfg();
        } catch (Exception e) {
            var out = new StringWriter();
            e.printStackTrace(new PrintWriter(out));
            throw new Exception("ERROR load `conveyor-sets.yml` failed - " + out.toString());
        }
        try {
            return loadIncludes(mainCfg);
        } catch (Exception e) {
            var out = new StringWriter();
            e.printStackTrace(new PrintWriter(out));
            throw new Exception("ERROR load includes failed - " + out.toString());
        }
    }

    private String reread() {
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            return e.getMessage();
        }
        var lines = new ArrayList<String>();
        var names = new LinkedHashSet<>(conveyorSetMap.keySet());
        for (var config : configs) {
            if (names.remove(config.name())) {
                var conveyorSet = conveyorSetMap.get(config.name());
                if (!conveyorSet.config().equals(config)) {
                    lines.add(config.name() + ": changed");
                }
            } else {
                lines.add(config.name() + ": available");
            }
        }
        for (var name : names) {
            lines.add(name + ": disappeared");
        }
        if (lines.isEmpty()) {
            lines.add("No config updates to processes");
        }
        return String.join("\r\n", lines);
    }

    private String startAll() {
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            return e.getMessage();
        }
        var lines = new ArrayList<String>();
        for (var config : configs) {
            if (!conveyorSetMap.containsKey(config.name())) {
                var conveyorSet = initConveyorSet(config);
                conveyorSet.startup(executor);
                conveyorSetMap.put(conveyorSet.name(), conveyorSet);
                lines.add(config.name() + ": started");
            }
        }
        if (lines.isEmpty()) {
            lines.add("No config updates to processes");
        }
        return String.join("\r\n", lines);
    }

    private String start(String name) {
        var conveyorSet = conveyorSetMap.get(name);
        if (conveyorSet != null) {
            return name + ": ERROR (already started)";
        }
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            return e.getMessage();
        }
        var config = configs.stream().filter(cfg -> name.equals(cfg.name())).findFirst();
        if (config.isEmpty()) {
            return name + ": ERROR (no such conveyor-set)";
        }
        conveyorSet = initConveyorSet(config.get());
        conveyorSetMap.put(conveyorSet.name(), conveyorSet);
        conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
        return name + ": started";
    }

    private String stopAll() {
        var lines = new ArrayList<String>();
        var names = new ArrayList<>(conveyorSetMap.keySet());
        var futures = new ArrayList<Future<Void>>(names.size());
        for (var name : names) {
            var conveyorSet = conveyorSetMap.remove(name);
            futures.add(conveyorSet.shutdown());
            lines.add(name + ": stopped");
        }
        if (lines.isEmpty()) {
            lines.add("No config updates to processes");
        } else {
            for (var future : futures) {
                future.awaitUninterruptibly(60, TimeUnit.SECONDS);
            }
        }
        return String.join("\r\n", lines);
    }

    private String stop(String name) {
        var conveyorSet = conveyorSetMap.remove(name);
        if (conveyorSet == null) {
            return name + ": ERROR (no such conveyor-set)";
        }
        conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
        return name + ": stopped";
    }

    private String restartAll() {
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            configs = List.of();
        }
        var lines = new ArrayList<String>();
        var names = new ArrayList<>(conveyorSetMap.keySet());
        for (var name : names) {
            var conveyorSet = conveyorSetMap.remove(name);
            conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
            lines.add(name + ": stopped");
            var newCfg = configs.stream().filter(cfg -> name.equals(cfg.name())).findFirst();
            if (newCfg.isPresent()) {
                conveyorSet = initConveyorSet(newCfg.get());
            } else {
                conveyorSet = initConveyorSet(conveyorSet.config());
            }
            conveyorSetMap.put(name, conveyorSet);
            conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
            lines.add(name + ": started");
        }
        if (lines.isEmpty()) {
            lines.add("No config updates to processes");
        }
        return String.join("\r\n", lines);
    }

    private String restart(String name) {
        var conveyorSet = conveyorSetMap.remove(name);
        if (conveyorSet == null) {
            return name + ": ERROR (no such conveyor-set)";
        }
        var lines = new ArrayList<String>();
        var future = conveyorSet.shutdown();
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            configs = List.of();
        }
        future.awaitUninterruptibly(60, TimeUnit.SECONDS);
        lines.add(name + ": stopped");
        var newCfg = configs.stream().filter(cfg -> name.equals(cfg.name())).findFirst();
        if (newCfg.isPresent()) {
            conveyorSet = initConveyorSet(newCfg.get());
        } else {
            conveyorSet = initConveyorSet(conveyorSet.config());
        }
        conveyorSetMap.put(name, conveyorSet);
        conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
        lines.add(name + ": started");
        return String.join("\r\n", lines);
    }

    private String updateAll() {
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            return e.getMessage();
        }
        var lines = new ArrayList<String>();
        var names = new LinkedHashSet<>(conveyorSetMap.keySet());
        for (var config : configs) {
            if (names.remove(config.name())) {
                var conveyorSet = conveyorSetMap.get(config.name());
                if (!conveyorSet.config().equals(config)) {
                    conveyorSetMap.remove(config.name());
                    conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
                    lines.add(config.name() + ": stopped");
                    conveyorSet = initConveyorSet(config);
                    conveyorSetMap.put(conveyorSet.name(), conveyorSet);
                    conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
                    lines.add(config.name() + ": started");
                }
            } else {
                var conveyorSet = initConveyorSet(config);
                conveyorSetMap.put(conveyorSet.name(), conveyorSet);
                conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
                lines.add(config.name() + ": started");
            }
        }
        for (var name : names) {
            var conveyorSet = conveyorSetMap.remove(name);
            conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
            lines.add(name + ": stopped");
        }
        if (lines.isEmpty()) {
            lines.add("No config updates to processes");
        }
        return String.join("\r\n", lines);
    }

    private String update(String name) {
        List<ConveyorSetConfig> configs;
        try {
            configs = loadIncludes();
        } catch (Exception e) {
            return e.getMessage();
        }
        if (conveyorSetMap.containsKey(name)) {
            var config = configs.stream().filter(cfg -> name.equals(cfg.name())).findFirst();
            if (config.isPresent()) {
                var newCfg = config.get();
                var conveyorSet = conveyorSetMap.get(name);
                if (newCfg.equals(conveyorSet.config())) {
                    return name + ": no changed";
                }
                conveyorSetMap.remove(name);
                var lines = new ArrayList<String>(2);
                conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
                lines.add(name + ": stopped");
                conveyorSet = initConveyorSet(config.get());
                conveyorSetMap.put(conveyorSet.name(), conveyorSet);
                conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
                lines.add(name + ": started");
                return String.join("\r\n", lines);
            }
            var conveyorSet = conveyorSetMap.remove(name);
            conveyorSet.shutdown().awaitUninterruptibly(60, TimeUnit.SECONDS);
            return name + ": stopped";
        }
        var config = configs.stream().filter(cfg -> name.equals(cfg.name())).findFirst();
        if (config.isPresent()) {
            var conveyorSet = initConveyorSet(config.get());
            conveyorSetMap.put(conveyorSet.name(), conveyorSet);
            conveyorSet.startup(executor).awaitUninterruptibly(60, TimeUnit.SECONDS);
            return name + ": started";
        }
        return name + ": ERROR (no such conveyor-set)";
    }

    private static final class StatusUtil {

        private static final String showStatus(ConveyorSet conveyorSet) {
            var lines = new ArrayList<String>(conveyorSet.conveyorSize() + 1);
            lines.add(format(toNameWidth(conveyorSet.name()), conveyorSet));
            var nameWidth = conveyorSet.conveyors().stream().mapToInt(c -> toNameWidth(c.name())).max().orElse(30);
            for (var iterator = conveyorSet.conveyors().listIterator(); iterator.hasNext();) {
                var conveyor = iterator.next();
                var isLast = !iterator.hasNext();
                var line = format(nameWidth, conveyor, isLast);
                lines.add(line);
            }
            return String.join("\r\n", lines);
        }

        private static final int toNameWidth(String name) {
            return Math.max(30, name.length());
        }

        private static final String format(int nameWidth, ConveyorSet cs) {
            var b = new StringBuilder();
            b.append(cs.name()).append(" ".repeat(nameWidth + 3 - cs.name().length())); // name
            b.append(cs.status()).append(" ".repeat(3)); // status
            b.append("conveyors ").append(cs.conveyorSize()).append(", "); // conveyor size
            b.append("uptime ").append(cs.upTime().map(upTime -> formatUpTime(upTime)).orElse("---")); // up time
            return b.toString();
        }

        private static final String format(int nameWidth, Conveyor conveyor, boolean isLast) {
            var b = new StringBuilder();
            b.append(isLast ? " └─ " : " ├─ ");
            b.append(conveyor.name()).append(" ".repeat(nameWidth + 2 - conveyor.name().length())); // name
            b.append(conveyor.status()); // status
            conveyor.threadInfo().ifPresent( // thread info
                    ti -> b.append(" ".repeat(2)).append("tid ").append(ti.id()).append(", tname ").append(ti.name()));
            var line = b.toString();
            return line;
        }

        private static final String formatUpTime(Duration upTime) {
            var b = new StringBuilder();
            var days = upTime.toDaysPart();
            if (days > 0) {
                b.append(days).append(" days, ");
            }
            var hours = upTime.toHoursPart();
            b.append(hours).append(":");
            var minutes = upTime.toMinutesPart();
            if (minutes < 10) {
                b.append("0");
            }
            b.append(minutes).append(":");
            var seconds = upTime.toSecondsPart();
            if (seconds < 10) {
                b.append("0");
            }
            b.append(seconds);
            return b.toString();
        }

    }
}
