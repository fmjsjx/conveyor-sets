package com.github.fmjsjx.conveyor.core;

import java.util.concurrent.Executor;

import io.netty.util.concurrent.Future;

public interface Conveyor {
    
    String name();

    boolean isStarted();

    boolean isRunning();

    Future<Void> shutdown();

    boolean isShuttingDown();

    boolean isTerminated();

    void startup(Executor executor);

}
