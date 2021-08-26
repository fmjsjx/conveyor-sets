package com.github.fmjsjx.conveyor.core;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.Future;

public interface ConveyorSet {
    
    String name();

    boolean isStarted();

    boolean isRunning();

    boolean isShuttingDown();

    boolean isTerminated();

    int conveyorSize();

    Future<ConveyorSet> startup(Executor executor);

    Future<Void> shutdown();

    default boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return shutdown().await(timeout, unit);
    }

}
