package com.github.fmjsjx.conveyor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerUtil {

    private static final class FailureLoggerHolder {
        private static final Logger INSTANCE = LoggerFactory.getLogger("failureLogger");
    }

    public static final Logger failureLogger() {
        return FailureLoggerHolder.INSTANCE;
    }

    private LoggerUtil() {
    }

}
