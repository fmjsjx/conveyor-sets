package com.github.fmjsjx.conveyor.core;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultConveyorSet implements ConveyorSet {

    private static final int NOT_STARTED = 0;
    private static final int RUNNING = 1;
    private static final int SHUTING_DOWN = 2;
    private static final int TERMINATED = 3;

    private final String name;
    private final List<Conveyor> conveyors;
    private final AtomicInteger stateCtl = new AtomicInteger(NOT_STARTED);

    private volatile Promise<Void> terminatedFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE)
            .setSuccess(null);

    public DefaultConveyorSet(String name, List<Conveyor> conveyors, Executor executor) {
        this(name, conveyors);
        startup(executor);
    }

    public DefaultConveyorSet(String name, List<Conveyor> conveyors) {
        this.name = name;
        this.conveyors = List.copyOf(conveyors);
    }
    
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isStarted() {
        return stateCtl.get() >= RUNNING;
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
    public String toString() {
        return "DefaultConveyorSet(" + name + ")";
    }

    @Override
    public synchronized void startup(Executor executor) {
        if (stateCtl.compareAndSet(NOT_STARTED, RUNNING)) {
            terminatedFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
            log.info("[conveyor-set:startup] Start up {}", this);
            for (Conveyor conveyor : conveyors) {
                conveyor.startup(executor);
            }
            log.info("[conveyor-set:startup] {} started", this);
        }
    }

    public synchronized Future<Void> shutdown() {
        if (stateCtl.compareAndSet(RUNNING, SHUTING_DOWN)) {
            log.info("[conveyor-set:shutdown] Shutdown {}", this);
            for (var conveyor : conveyors) {
                conveyor.shutdown().addListener(f -> shutdownTrigger());
            }
        }
        return terminatedFuture;
    }

    private void shutdownTrigger() {
        if (conveyors.stream().allMatch(Conveyor::isTerminated)) {
            terminatedFuture.trySuccess(null);
        }
    }

}
