package com.github.fmjsjx.conveyor.core;

import java.lang.Thread.State;

public interface ThreadInfo {

    long id();

    String name();

    String groupName();

    boolean daemon();

    State state();

}
