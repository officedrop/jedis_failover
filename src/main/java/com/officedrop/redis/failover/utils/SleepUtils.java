package com.officedrop.redis.failover.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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

}
