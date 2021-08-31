package com.github.fmjsjx.conveyor.admin;

import io.netty.util.concurrent.Future;

public interface AdminServer {

    Future<?> shutdown();

}
