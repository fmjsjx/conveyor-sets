package com.github.fmjsjx.conveyor.core;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.github.fmjsjx.conveyor.config.ConveyorSetConfig;

import io.netty.util.concurrent.Future;

public interface ConveyorSet {
    
    default String name() {
        return config().name();
    }
    
    ConveyorSetConfig config();
    
    Optional<Duration> upTime();

    boolean isStarted();

    boolean isRunning();

    boolean isShuttingDown();

    boolean isTerminated();

    default int conveyorSize() {
        return conveyors().size();
    }
    
    List<Conveyor> conveyors();

    Future<ConveyorSet> startup(Executor executor);

    Future<Void> shutdown();

    default boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return shutdown().await(timeout, unit);
    }

    String status();

}
