package com.github.fmjsjx.conveyor.core;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.github.fmjsjx.conveyor.config.ConveyorSetConfig;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultConveyorSet implements ConveyorSet {

    private static final int NOT_STARTED = 0;
    private static final int STARTED = 1;
    private static final int RUNNING = 2;
    private static final int SHUTING_DOWN = 3;
    private static final int TERMINATED = 4;

    private final ConveyorSetConfig config;
    private final List<Conveyor> conveyors;
    private final AtomicInteger stateCtl = new AtomicInteger(NOT_STARTED);
    private final AtomicReference<LocalDateTime> startTimeRef = new AtomicReference<>();

    private final Promise<ConveyorSet> runningFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);

    private volatile Promise<Void> terminatedFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE)
            .setSuccess(null);

    public DefaultConveyorSet(ConveyorSetConfig config, List<Conveyor> conveyors, Executor executor) {
        this(config, conveyors);
        startup(executor);
    }

    public DefaultConveyorSet(ConveyorSetConfig config, List<Conveyor> conveyors) {
        this.config = config;
        this.conveyors = List.copyOf(conveyors);
    }

    @Override
    public ConveyorSetConfig config() {
        return config;
    }

    @Override
    public Optional<Duration> upTime() {
        var startTime = startTimeRef.get();
        if (startTime == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.between(startTime, LocalDateTime.now()));
    }

    @Override
    public boolean isStarted() {
        return stateCtl.get() >= STARTED;
    }

    @Override
    public boolean isRunning() {
        return stateCtl.get() == RUNNING;
    }

    @Override
    public boolean isShuttingDown() {
        return stateCtl.get() >= SHUTING_DOWN;
    }

    @Override
    public boolean isTerminated() {
        return stateCtl.get() == TERMINATED;
    }

    @Override
    public int conveyorSize() {
        return conveyors.size();
    }

    @Override
    public List<Conveyor> conveyors() {
        return conveyors;
    }
    
    @Override
    public String toString() {
        return "DefaultConveyorSet(" + name() + ")";
    }

    @Override
    public String status() {
        var state = stateCtl.get();
        switch (state) {
        case NOT_STARTED:
            return "NOT_STARTED";
        case STARTED:
            return "STARTED";
        case RUNNING:
            return "RUNNING";
        case SHUTING_DOWN:
            return "SHUTING_DOWN";
        case TERMINATED:
            return "TERMINATED";
        default:
            return "UNKNOWN(" + state + ")";
        }
    }

    @Override
    public synchronized Future<ConveyorSet> startup(Executor executor) {
        if (stateCtl.compareAndSet(NOT_STARTED, STARTED)) {
            startTimeRef.set(LocalDateTime.now());
            terminatedFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
            log.info("[conveyor-set:startup] Start up {}", this);
            for (Conveyor conveyor : conveyors) {
                conveyor.startup(executor).addListener(f -> runningTrigger());
            }
            log.info("[conveyor-set:startup] {} started", this);
        }
        return runningFuture;
    }

    private void runningTrigger() {
        if (conveyors.stream().allMatch(Conveyor::isRunning)) {
            stateCtl.set(RUNNING);
            runningFuture.trySuccess(this);
        }
    }

    public synchronized Future<Void> shutdown() {
        if (stateCtl.compareAndSet(RUNNING, SHUTING_DOWN)) {
            // shutdown if is running
            shutdown0();
        } else if (stateCtl.compareAndSet(STARTED, SHUTING_DOWN)) {
            // not running, shutdown if is just started
            shutdown0();
        } else if (stateCtl.compareAndSet(RUNNING, SHUTING_DOWN)) {
            // double check for running
            shutdown0();
        } else {
            // if not started, just set to terminated
            stateCtl.compareAndSet(NOT_STARTED, TERMINATED);
        }
        return terminatedFuture;
    }

    private void shutdown0() {
        log.info("[conveyor-set:shutdown] Shutdown {}", this);
        for (var conveyor : conveyors) {
            conveyor.shutdown().addListener(f -> shutdownTrigger());
        }
    }

    private void shutdownTrigger() {
        if (conveyors.stream().allMatch(Conveyor::isTerminated)) {
            if (stateCtl.compareAndSet(SHUTING_DOWN, TERMINATED)) {
                // terminated properly
                log.info("[conveyor-set:shutdown] {} terminated properly", this);
                terminatedFuture.trySuccess(null);
            } else {
                stateCtl.set(TERMINATED);
                if (terminatedFuture.trySuccess(null)) {
                    log.info("[conveyor-set:shutdown] {} terminated", this);
                }
            }
        }
    }

}
