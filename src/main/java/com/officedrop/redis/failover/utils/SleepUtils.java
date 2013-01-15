package com.officedrop.redis.failover.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 7:25 PM
 */
public class SleepUtils {

    private static final Logger log = LoggerFactory.getLogger(SleepUtils.class);

    public static final void safeSleep( long time, TimeUnit unit) {

        try {
            unit.sleep(time);
        } catch ( InterruptedException e ) {
            log.error("Interrupted while sleeping", e);
        }

    }

    public static final void waitUntil( long millis, Function<Boolean> waiter ) {

        long totalSlept = 0;

        while ( totalSlept <= millis && !waiter.apply() ) {
            safeSleep(1, TimeUnit.SECONDS);
            totalSlept += 1000;
        }

        if ( !waiter.apply() ) {
            throw new IllegalStateException(String.format("Execution of loop timed out after %s millis", millis));
        }

    }

}
