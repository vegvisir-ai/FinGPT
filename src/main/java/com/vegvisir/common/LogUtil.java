package com.vegvisir.common;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtil {
    public enum LogLevel {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    }

    public static void log(LambdaLogger logger, LogLevel level, String message) {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        String formattedTime = dateFormat.format(now);

        logger.log(String.format("{%s} [%s] %s\n", formattedTime, level, message));
    }
}
