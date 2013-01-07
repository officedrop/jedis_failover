package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 12/26/12
 * Time: 5:19 PM
 */
public class Benchmarker {

    public static long benchmark( Action action ) {
        long currentTime = System.currentTimeMillis();
        action.apply();
        return System.currentTimeMillis() - currentTime;
    }

}
