package com.github.fmjsjx.conveyor.core;

import java.util.Optional;
import java.util.concurrent.Executor;

import io.netty.util.concurrent.Future;

public interface Conveyor {

    String name();

    boolean isStarted();

    boolean isRunning();

    Future<Void> shutdown();

    boolean isShuttingDown();

    boolean isTerminated();

    Future<Conveyor> startup(Executor executor);

    String status();

    Optional<ThreadInfo> threadInfo();

}
