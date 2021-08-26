package com.github.fmjsjx.conveyor.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.fmjsjx.conveyor.core.input.Input;
import com.github.fmjsjx.conveyor.core.output.Output;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultConveyor implements Conveyor {

    private static final int NOT_STARTED = 0;
    private static final int STARTED = 1;
    private static final int RUNNING = 2;
    private static final int SHUTING_DOWN = 3;
    private static final int TERMINATED = 4;

    private final String name;
    private final Input input;
    private final Output output;
    private final AtomicInteger stateCtl = new AtomicInteger(NOT_STARTED);
    private final Promise<Conveyor> runningFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);

    private volatile Promise<Void> terminatedFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE)
            .setSuccess(null);

    public DefaultConveyor(String name, Input input, Output output, Executor executor) {
        this(name, input, output);
        startup(executor);
    }

    public DefaultConveyor(String name, Input input, Output output) {
        this.name = name;
        this.input = input;
        this.output = output;
    }

    @Override
    public String name() {
        return name;
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
    public synchronized Future<Void> shutdown() {
        if (!stateCtl.compareAndSet(RUNNING, SHUTING_DOWN)) {
            // if just started then set to terminated
            if (!stateCtl.compareAndSet(STARTED, SHUTING_DOWN)) {
                // double check for running state
                if (!stateCtl.compareAndSet(RUNNING, SHUTING_DOWN)) {
                    // if not started, just set to terminated
                    stateCtl.compareAndSet(NOT_STARTED, TERMINATED);
                }
            }
        }
        return terminatedFuture;
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
    public String toString() {
        return "DefaultConveyor(" + name + ")";
    }

    @Override
    public synchronized Future<Conveyor> startup(Executor executor) {
        if (stateCtl.compareAndSet(NOT_STARTED, STARTED)) {
            terminatedFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
            executor.execute(this::start);
        }
        return runningFuture;
    }

    private void start() {
        stateCtl.set(RUNNING);
        log.info("[conveyor:startup] Start up {}", this);
        runningFuture.setSuccess(this);
        for (; isRunning();) {
            safeTransfer();
        }
        log.info("[conveyor:shutdown] Shutdown {}", this);
        var input = this.input;
        log.info("[conveyor:shutdown] Close {}", input);
        try {
            input.close();
        } catch (Exception e) {
            log.warn("[conveyor:shutdown] Unexpected error occurs when close {}", input, e);
        }
        var output = this.output;
        log.info("[conveyor:shutdown] Close {}", output);
        try {
            output.close();
        } catch (Exception e) {
            log.warn("[conveyor:shutdown] Unexpected error occurs when close {}", output, e);
        }
        if (stateCtl.compareAndSet(SHUTING_DOWN, TERMINATED)) {
            // terminated properly
            log.info("[conveyor:shutdown] {} terminated properly", this);
            terminatedFuture.setSuccess(null);
        } else {
            stateCtl.set(TERMINATED);
            terminatedFuture.trySuccess(null);
            log.info("[conveyor:shutdown] {} terminated", this);
        }
    }

    private void safeTransfer() {
        var batch = safeInputFetch();
        if (batch.isEmpty()) {
            // just returns when batch is empty
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("[conveyor:input] Fetched data(size={}) from {}: {}", batch.size(), input, batch);
        }
        safeOutputPush(batch);
    }

    private List<Map<String, String>> safeInputFetch() {
        var input = this.input;
        for (int retryCount = 0; retryCount < 3; retryCount++) {
            try {
                return input.fetch();
            } catch (Exception e) {
                log.error("[conveyor:input] Unexpected error occurs when fetch data from {}", input, e);
            }
        }
        if (isRunning()) {
            // sleep 5 seconds then retry last once
            try {
                Thread.sleep(5 * 1000);
                return input.fetch();
            } catch (Exception e) {
                log.error("[conveyor:input] Unexpected error occurs when fetch data from {}", input, e);
            }
        }
        // just returns empty data
        return List.of();
    }

    private void safeOutputPush(List<Map<String, String>> batch) {
        var output = this.output;
        for (var retryCount = 0; isRunning(); retryCount++) {
            // always retry when running
            try {
                output.push(batch);
                return;
            } catch (Exception e) {
                log.error("[conveyor:output] Unexpected error occurs when push data to {}", output, e);
                if (retryCount > 3) {
                    // sleep 30 seconds max
                    var sleepMillis = retryCount > 9 ? 30_000 : (retryCount - 3) * 5_000;
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (Exception ex) {
                        // skip
                    }
                }
            }
        }
        // push failed but never running
        output.failed(batch);
    }

}
